#include "raft/fake_cluster.h"

#include <cstdio>

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

raft::NodeId elect_leader(raft::FakeCluster& cluster, raft::NodeId candidate) {
    cluster.inject(candidate, 0, raft::ElectionTimeout{});
    cluster.drain_random();
    return candidate;
}

// ---------------------------------------------------------------------------
// Hand-constructed follower-side conflict resolution, exercised directly
// through handle() (not via FakeCluster) — the design doc's own build-order
// note asks for exactly this: "write it against deliberately constructed
// conflicting-log test cases."
// ---------------------------------------------------------------------------

void test_conflict_detected_and_truncated() {
    raft::RaftState follower;
    follower.self_id = 1;
    follower.log.append(1, "a");  // index 1, term 1
    follower.log.append(1, "b");  // index 2, term 1
    follower.log.append(1, "c");  // index 3, term 1 — from a stale leader

    raft::AppendEntriesRequest req;
    req.term = 2;
    req.leader_id = 0;
    req.prev_log_index = 2;
    req.prev_log_term = 1;  // matches follower's index 2 — passes consistency check
    req.entries = {
        raft::LogEntry{2, "replacement_c"},  // conflicts with existing index 3 (term 1 vs 2)
        raft::LogEntry{2, "d"},              // new, beyond the old log's end
    };

    const raft::HandleResult r =
        raft::handle(follower, 0, raft::Received<raft::AppendEntriesRequest>{req}, 3);

    const auto* resp = std::get_if<raft::AppendEntriesResponse>(&r.messages[0].message);
    CHECK(resp != nullptr && resp->success);
    CHECK(r.truncated_to.has_value() && *r.truncated_to == 3);

    CHECK(follower.log.last_index() == 4);
    CHECK(follower.log.at(1)->command == "a");         // untouched
    CHECK(follower.log.at(2)->command == "b");         // untouched
    CHECK(follower.log.at(3)->term == 2);
    CHECK(follower.log.at(3)->command == "replacement_c");  // old term-1 entry GONE
    CHECK(follower.log.at(4)->command == "d");
}

// The safe case: term matches at the shared index, so no truncation should
// occur — Step 4's original duplicate-skip behavior, still correct here.
void test_matching_term_no_truncation() {
    raft::RaftState follower;
    follower.self_id = 1;
    follower.log.append(1, "a");
    follower.log.append(1, "b");

    raft::AppendEntriesRequest req;
    req.term = 1;
    req.leader_id = 0;
    req.prev_log_index = 0;
    req.prev_log_term = 0;
    req.entries = {raft::LogEntry{1, "a"}, raft::LogEntry{1, "b"}};  // identical retry

    const raft::HandleResult r =
        raft::handle(follower, 0, raft::Received<raft::AppendEntriesRequest>{req}, 3);

    CHECK(!r.truncated_to.has_value());
    CHECK(follower.log.last_index() == 2);
    CHECK(follower.log.at(1)->command == "a");
    CHECK(follower.log.at(2)->command == "b");
}

// ---------------------------------------------------------------------------
// End-to-end recovery through FakeCluster: manually diverge a follower's
// log (simulating what a stale leader would have written), then let the
// real leader's normal propose+retry machinery recover it — no test code
// directly fixes the follower; only handle()'s own backtrack logic does.
// ---------------------------------------------------------------------------

void test_diverged_follower_recovers_via_backtrack() {
    raft::FakeCluster cluster(3, /*seed=*/11);
    const raft::NodeId leader = elect_leader(cluster, 0);
    CHECK(cluster.node(leader).role == raft::Role::kLeader);

    // Stale entries need a genuinely LOWER term than what the leader
    // will propose — same-term entries at the same index are guaranteed
    // identical by Raft's own invariants, so using the leader's current
    // term here (as an earlier version of this test did) can never
    // trigger real conflict detection, only the harmless-duplicate path.
    const raft::Term stale_term = cluster.node(leader).current_term;
    cluster.node(1).log.append(stale_term, "stale_entry_1");
    cluster.node(1).log.append(stale_term, "stale_entry_2");
    cluster.node(leader).current_term += 5;

    cluster.propose(leader, "real_entry_1");
    cluster.drain_random();
    cluster.propose(leader, "real_entry_2");
    cluster.drain_random();

    CHECK(raft::logs_equal(cluster.node(leader), cluster.node(1)));
    CHECK(cluster.node(1).log.at(1)->command == "real_entry_1");
    CHECK(cluster.node(1).log.at(2)->command == "real_entry_2");
    CHECK(cluster.node(1).log.last_index() == 2);
}

// A follower missing several entries (not conflicting, just behind) must
// fully catch up over several backtrack/continuation round trips.
void test_far_behind_follower_catches_up() {
    raft::FakeCluster cluster(3, /*seed=*/12);
    const raft::NodeId leader = elect_leader(cluster, 0);

    // Drop every message to/from node 2 for the first three proposes, so
    // it falls behind while node 1 stays caught up.
    for (int i = 0; i < 3; ++i) {
        cluster.propose(leader, "cmd" + std::to_string(i));
        cluster.drop_one([](const raft::InFlight& m) { return m.to == 2 || m.from == 2; });
        cluster.drain_random();
    }
    CHECK(cluster.node(2).log.last_index() < cluster.node(leader).log.last_index());

    // One more propose, fully delivered this time — node 2's own
    // rejection-driven backtrack should now catch it all the way up,
    // without any further drops.
    cluster.propose(leader, "cmd_final");
    // Drain generously — catch-up takes one round trip per missing entry.
    cluster.drain_random(200);

    CHECK(raft::logs_equal(cluster.node(leader), cluster.node(2)));
}

// ---------------------------------------------------------------------------
// nextIndex/matchIndex bookkeeping itself, inspected directly.
// ---------------------------------------------------------------------------

void test_tracking_seeded_lazily_not_eagerly() {
    raft::FakeCluster cluster(3, /*seed=*/13);
    cluster.inject(0, 0, raft::ElectionTimeout{});

    // elect_leader() fully drains internally, which would already seed
    // tracking via first-contact before we get to inspect it — so we
    // step manually instead, stopping the instant leadership is won.
    while (cluster.node(0).role != raft::Role::kLeader && !cluster.queue_empty()) {
        cluster.tick_random();
    }
    CHECK(cluster.node(0).role == raft::Role::kLeader);

    CHECK(cluster.node(0).next_index.empty());
    CHECK(cluster.node(0).match_index.empty());

    cluster.drain_random();  // now let the heartbeat's responses through

    CHECK(cluster.node(0).next_index.count(1) == 1);
    CHECK(cluster.node(0).next_index.count(2) == 1);
    CHECK(cluster.node(0).match_index.at(1) == 0);
    CHECK(cluster.node(0).match_index.at(2) == 0);
}

void test_matchindex_advances_after_successful_propose() {
    raft::FakeCluster cluster(3, /*seed=*/14);
    const raft::NodeId leader = elect_leader(cluster, 0);
    cluster.drain_random();  // let first-contact tracking seed

    cluster.propose(leader, "cmd1");
    cluster.drain_random();

    CHECK(cluster.node(leader).match_index.at(1) == 1);
    CHECK(cluster.node(leader).match_index.at(2) == 1);
    CHECK(cluster.node(leader).next_index.at(1) == 2);
    CHECK(cluster.node(leader).next_index.at(2) == 2);
}

// A fresh election must reset stale tracking from a prior term — a node
// that lost leadership and later regains it should not reuse old
// bookkeeping about followers that may have changed in the meantime.
void test_new_election_clears_prior_tracking() {
    raft::RaftState s;
    s.self_id = 0;
    s.role = raft::Role::kLeader;
    s.current_term = 3;
    s.next_index[1] = 5;
    s.match_index[1] = 4;

    // Step down via a higher-term response — this leaves next_index/
    // match_index untouched (maybe_update_term doesn't clear them);
    // they just sit unused while role != Leader.
    raft::AppendEntriesResponse higher_term_resp;
    higher_term_resp.term = 9;
    higher_term_resp.success = false;
    raft::handle(s, 1, raft::Received<raft::AppendEntriesResponse>{higher_term_resp}, 3);
    CHECK(s.role == raft::Role::kFollower);

    // NOW a timeout is real — role is Follower, so the early-return
    // guard doesn't apply, and this is what should clear tracking.
    raft::handle(s, 0, raft::ElectionTimeout{}, 3);

    CHECK(s.role == raft::Role::kCandidate);
    CHECK(s.next_index.empty());
    CHECK(s.match_index.empty());
}

}  // namespace

int main() {
    test_conflict_detected_and_truncated();
    test_matching_term_no_truncation();

    test_diverged_follower_recovers_via_backtrack();
    test_far_behind_follower_catches_up();

    test_tracking_seeded_lazily_not_eagerly();
    test_matchindex_advances_after_successful_propose();
    test_new_election_clears_prior_tracking();

    if (g_failures == 0) {
        std::printf("backtrack_test: all checks passed\n");
        return 0;
    }
    std::fprintf(stderr, "backtrack_test: %d check(s) failed\n", g_failures);
    return 1;
}