#include "raft/fake_cluster.h"

#include <cstdio>
#include <cstdint>

namespace {

int g_failures = 0;

#define CHECK(cond)                                                          \
    do {                                                                     \
        if (!(cond)) {                                                       \
            std::fprintf(stderr, "%s:%d: CHECK failed: %s\n", __FILE__,      \
                         __LINE__, #cond);                                   \
            ++g_failures;                                                    \
        }                                                                    \
    } while (0)

bool is_request_vote(const raft::InFlight& m) {
    return std::holds_alternative<raft::RequestVoteRequest>(m.message);
}

// ---------------------------------------------------------------------------
// Basic wiring: a single election timeout, delivered FIFO, converges.
// This is the §2.3 worked example, executed for real.
// ---------------------------------------------------------------------------

void test_single_candidate_wins_fifo() {
    raft::FakeCluster cluster(3, /*seed=*/1);

    cluster.inject(0, 0, raft::ElectionTimeout{});
    CHECK(cluster.queue_size() == 2);  // RequestVote to node 1 and node 2

    const std::size_t delivered = cluster.drain_random();
    CHECK(delivered > 0);
    CHECK(cluster.queue_empty());

    CHECK(cluster.node(0).role == raft::Role::kLeader);
    CHECK(cluster.node(1).role == raft::Role::kFollower);
    CHECK(cluster.node(2).role == raft::Role::kFollower);
    CHECK(cluster.node(0).current_term == 1);
    CHECK(cluster.node(1).current_term == 1);
    CHECK(cluster.node(2).current_term == 1);

    CHECK(raft::has_single_leader_for_latest_term(cluster));
}

// ---------------------------------------------------------------------------
// The convergence property must hold regardless of delivery order. Run many
// seeds; a failure on any seed is reproducible by re-running that seed alone
// (design notes §3.3).
// ---------------------------------------------------------------------------

void test_election_converges_across_many_orderings() {
    for (std::uint64_t seed = 0; seed < 500; ++seed) {
        raft::FakeCluster cluster(3, seed);
        cluster.inject(0, 0, raft::ElectionTimeout{});
        cluster.drain_random();

        if (!raft::has_single_leader_for_latest_term(cluster)) {
            std::fprintf(stderr,
                         "test_election_converges_across_many_orderings: "
                         "FAILED to converge with seed=%llu\n",
                         static_cast<unsigned long long>(seed));
            ++g_failures;
        }
    }
}

// Same property, but in a 5-node cluster (majority = 3, not 2) — the
// majority threshold arithmetic is a common off-by-one site.
void test_election_converges_five_node_cluster() {
    for (std::uint64_t seed = 0; seed < 200; ++seed) {
        raft::FakeCluster cluster(5, seed);
        cluster.inject(0, 0, raft::ElectionTimeout{});
        cluster.drain_random();

        if (!raft::has_single_leader_for_latest_term(cluster)) {
            std::fprintf(stderr,
                         "test_election_converges_five_node_cluster: "
                         "FAILED to converge with seed=%llu\n",
                         static_cast<unsigned long long>(seed));
            ++g_failures;
        }
    }
}

// ---------------------------------------------------------------------------
// Engineered split vote (design notes §2.5): two nodes time out before
// either sees the other's request. At most one can win term 1; if neither
// does, both must be able to retry and eventually converge in a later term.
// ---------------------------------------------------------------------------

void test_split_vote_then_retry_converges() {
    raft::FakeCluster cluster(3, /*seed=*/42);

    // Both node 1 and node 2 time out "simultaneously" — neither has seen
    // the other's RequestVoteRequest yet, since nothing has been delivered.
    cluster.inject(1, 0, raft::ElectionTimeout{});
    cluster.inject(2, 0, raft::ElectionTimeout{});

    CHECK(cluster.node(1).role == raft::Role::kCandidate);
    CHECK(cluster.node(2).role == raft::Role::kCandidate);
    CHECK(cluster.node(1).current_term == 1);
    CHECK(cluster.node(2).current_term == 1);

    // Drain everything from this round. Node 0 will grant its single vote
    // to whichever of the two requests it happens to see first; the other
    // candidate cannot reach a majority of 2-out-of-3 this term.
    cluster.drain_random();

    const bool one_leader_this_term =
        (cluster.node(1).role == raft::Role::kLeader) !=
        (cluster.node(2).role == raft::Role::kLeader);
    const bool split_vote_no_leader =
        cluster.node(1).role != raft::Role::kLeader &&
        cluster.node(2).role != raft::Role::kLeader;

    // Either outcome is a legal execution depending on delivery order —
    // what must NEVER happen is both claiming leadership simultaneously.
    CHECK(!(cluster.node(1).role == raft::Role::kLeader &&
            cluster.node(2).role == raft::Role::kLeader));
    CHECK(one_leader_this_term || split_vote_no_leader);

    if (split_vote_no_leader) {
        // Genuine split vote: neither candidate got a majority. Retry with
        // a fresh timeout should let one of them win.
        cluster.inject(1, 0, raft::ElectionTimeout{});
        cluster.drain_random();
        CHECK(raft::has_single_leader_for_latest_term(cluster));
    }
}

// ---------------------------------------------------------------------------
// A dropped message must not prevent convergence, as long as enough OTHER
// messages get through for a majority (design notes §4's scope note: only
// single-message drops are modeled here, not full partitions).
// ---------------------------------------------------------------------------

void test_convergence_survives_one_dropped_vote_response() {
    raft::FakeCluster cluster(3, /*seed=*/7);
    cluster.inject(0, 0, raft::ElectionTimeout{});

    // Drop node 2's RequestVoteRequest entirely — node 2 never gets to
    // vote. Node 0 must still win using node 0's self-vote + node 1's
    // grant (2 out of 3 is a majority; losing node 2's vote is survivable).
    const bool dropped = cluster.drop_one([](const raft::InFlight& m) {
        return m.to == 2 && is_request_vote(m);
    });
    CHECK(dropped);

    cluster.drain_random();

    CHECK(cluster.node(0).role == raft::Role::kLeader);
    // Node 2's own RequestVoteRequest was dropped, so it never got to
    // vote — but it still receives the leader's post-election heartbeat
    // (broadcast reaches every other node, not just the ones who voted),
    // so its term catches up to the leader's even though it took no part
    // in the election itself.
    CHECK(cluster.node(2).role == raft::Role::kFollower);
    CHECK(cluster.node(2).current_term == cluster.node(0).current_term);
}

// ---------------------------------------------------------------------------
// A leader partitioned away and returning (design notes' worked example,
// mirrored from Step 2a §2.2, now exercised across real nodes).
// ---------------------------------------------------------------------------

void test_stale_leader_steps_down_on_rejoining() {
    raft::FakeCluster cluster(3, /*seed=*/99);

    // Node 0 wins term 1 normally.
    cluster.inject(0, 0, raft::ElectionTimeout{});
    cluster.drain_random();
    CHECK(cluster.node(0).role == raft::Role::kLeader);

    // Simulate node 0 being partitioned away: nodes 1 and 2 time out and
    // elect node 1 as the new leader in term 2, without node 0 hearing
    // any of it (we simply never deliver to/from node 0 during this part).
    cluster.inject(1, 0, raft::ElectionTimeout{});
    cluster.tick_while_matching(
        [](const raft::InFlight& m) { return m.to != 0 && m.from != 0; });

    CHECK(cluster.node(1).role == raft::Role::kLeader);
    CHECK(cluster.node(1).current_term == 2);
    CHECK(cluster.node(0).current_term == 1);  // still hasn't heard anything
    CHECK(cluster.node(0).role == raft::Role::kLeader);  // still thinks it leads

    // Partition heals: node 0's stale heartbeat (term 1) reaches node 1.
    // Node 1 rejects it (term 1 < term 2) and node 0 learns the real term
    // from the rejection response.
    cluster.inject(0, 0, raft::ElectionTimeout{});  // not realistic on its
                                                    // own — instead, drive
                                                    // the actual reconciliation:
    // Directly simulate node 0's stale heartbeat reaching node 1:
    raft::AppendEntriesRequest stale_heartbeat;
    stale_heartbeat.term = 1;
    stale_heartbeat.leader_id = 0;
    cluster.inject(1, 0, raft::Received<raft::AppendEntriesRequest>{stale_heartbeat});
    cluster.drain_random();

    CHECK(cluster.node(0).role == raft::Role::kFollower);
    CHECK(cluster.node(0).current_term >= 2);
}

}  // namespace

int main() {
    test_single_candidate_wins_fifo();
    test_election_converges_across_many_orderings();
    test_election_converges_five_node_cluster();
    test_split_vote_then_retry_converges();
    test_convergence_survives_one_dropped_vote_response();
    test_stale_leader_steps_down_on_rejoining();

    if (g_failures == 0) {
        std::printf("fake_cluster_test: all checks passed\n");
        return 0;
    }
    std::fprintf(stderr, "fake_cluster_test: %d check(s) failed\n", g_failures);
    return 1;
}