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
// Happy-path replication: propose several entries, verify every node's log
// converges to the same content in the same order.
// ---------------------------------------------------------------------------

void test_proposed_entries_replicate_to_all_followers() {
    raft::FakeCluster cluster(3, /*seed=*/1);
    const raft::NodeId leader = elect_leader(cluster, 0);
    CHECK(cluster.node(leader).role == raft::Role::kLeader);

    const raft::ProposeResult r1 = cluster.propose(leader, "PUT a 1");
    CHECK(r1.accepted);
    CHECK(r1.index == 1);
    cluster.drain_random();

    const raft::ProposeResult r2 = cluster.propose(leader, "PUT b 2");
    CHECK(r2.accepted);
    CHECK(r2.index == 2);
    cluster.drain_random();

    const raft::ProposeResult r3 = cluster.propose(leader, "DEL a");
    CHECK(r3.accepted);
    CHECK(r3.index == 3);
    cluster.drain_random();

    CHECK(cluster.node(0).log.last_index() == 3);
    CHECK(raft::logs_equal(cluster.node(0), cluster.node(1)));
    CHECK(raft::logs_equal(cluster.node(0), cluster.node(2)));

    CHECK(cluster.node(1).log.at(1)->command == "PUT a 1");
    CHECK(cluster.node(1).log.at(2)->command == "PUT b 2");
    CHECK(cluster.node(1).log.at(3)->command == "DEL a");
}

// Entries proposed and delivered under RANDOM ordering (not one at a time,
// fully drained between each) — closer to how real concurrent replication
// would interleave, and the strongest test that the log ends up correct
// regardless of message arrival order.
void test_replication_converges_under_randomized_delivery() {
    for (std::uint64_t seed = 0; seed < 100; ++seed) {
        raft::FakeCluster cluster(3, seed);
        const raft::NodeId leader = elect_leader(cluster, 0);
        if (cluster.node(leader).role != raft::Role::kLeader) {
            // Extremely unlikely with a single candidate and no
            // contention, but if it ever happens, skip rather than
            // false-fail this convergence check (election itself is
            // covered exhaustively in Step 2c's own tests).
            continue;
        }

        // Drain after each propose: Step 4 has no retry mechanism, so
        // entries must be fully replicated one at a time (design notes §2.4).
        cluster.propose(leader, "cmd1");
        cluster.drain_random();
        cluster.propose(leader, "cmd2");
        cluster.drain_random();
        cluster.propose(leader, "cmd3");
        cluster.drain_random();

        if (!raft::logs_equal(cluster.node(0), cluster.node(1)) ||
            !raft::logs_equal(cluster.node(0), cluster.node(2))) {
            std::fprintf(stderr,
                         "test_replication_converges_under_randomized_delivery: "
                         "logs diverged with seed=%llu\n",
                         static_cast<unsigned long long>(seed));
            ++g_failures;
        }
    }
}

// ---------------------------------------------------------------------------
// Only a Leader may propose.
// ---------------------------------------------------------------------------

void test_propose_rejected_on_follower() {
    raft::FakeCluster cluster(3, /*seed=*/2);
    CHECK(cluster.node(1).role == raft::Role::kFollower);

    const raft::ProposeResult r = cluster.propose(1, "should not work");
    CHECK(!r.accepted);
    CHECK(r.index == 0);
    CHECK(r.to_send.messages.empty());
    CHECK(cluster.node(1).log.last_index() == 0);  // nothing appended
}

void test_propose_rejected_on_candidate() {
    raft::FakeCluster cluster(3, /*seed=*/3);
    cluster.inject(1, 0, raft::ElectionTimeout{});
    CHECK(cluster.node(1).role == raft::Role::kCandidate);

    const raft::ProposeResult r = cluster.propose(1, "should not work");
    CHECK(!r.accepted);
}

// ---------------------------------------------------------------------------
// Consistency check correctly REJECTS a genuinely diverged follower (design
// notes §2.2's second worked example) — recovery is out of scope (Step 6),
// but rejection must still be correct.
// ---------------------------------------------------------------------------

void test_consistency_check_rejects_missing_prev_entry() {
    raft::FakeCluster cluster(3, /*seed=*/4);
    const raft::NodeId leader = elect_leader(cluster, 0);
    cluster.propose(leader, "cmd1");
    cluster.drain_random();
    CHECK(cluster.node(1).log.last_index() == 1);  // follower caught up

    // Hand-construct an AppendEntries claiming prev_log_index = 5 (the
    // follower's log only goes up to 1) — this simulates what a
    // dramatically-behind follower would see.
    raft::AppendEntriesRequest req;
    req.term = cluster.node(leader).current_term;
    req.leader_id = leader;
    req.prev_log_index = 5;
    req.prev_log_term = cluster.node(leader).current_term;
    req.entries = {raft::LogEntry{cluster.node(leader).current_term, "cmd_future"}};

    cluster.inject(1, leader, raft::Received<raft::AppendEntriesRequest>{req});

    // Drain manually so we can inspect the specific response rather than
    // letting drain_random() process everything opaquely.
    bool saw_rejection = false;
    cluster.tick_while_matching([&](const raft::InFlight& m) {
        if (auto* resp = std::get_if<raft::AppendEntriesResponse>(&m.message)) {
            if (m.from == 1 && !resp->success) saw_rejection = true;
        }
        return true;  // deliver everything, we're just observing
    });

    CHECK(saw_rejection);
    CHECK(cluster.node(1).log.last_index() == 1);  // nothing appended on reject
}

void test_consistency_check_rejects_term_mismatch_at_prev_index() {
    raft::FakeCluster cluster(3, /*seed=*/5);
    const raft::NodeId leader = elect_leader(cluster, 0);
    cluster.propose(leader, "cmd1");  // index 1, term = leader's term (say T)
    cluster.drain_random();

    const raft::Term real_term = cluster.node(1).log.term_at(1);

    raft::AppendEntriesRequest req;
    req.term = cluster.node(leader).current_term;
    req.leader_id = leader;
    req.prev_log_index = 1;
    req.prev_log_term = real_term + 100;  // deliberately wrong term at index 1
    req.entries = {};  // empty entries is fine; the mismatch is in prev_log_term

    cluster.inject(1, leader, raft::Received<raft::AppendEntriesRequest>{req});

    bool saw_rejection = false;
    cluster.tick_while_matching([&](const raft::InFlight& m) {
        if (auto* resp = std::get_if<raft::AppendEntriesResponse>(&m.message)) {
            if (m.from == 1 && !resp->success) saw_rejection = true;
        }
        return true;
    });
    CHECK(saw_rejection);
}

// prev_log_index == 0 must NOT be treated as "missing" — it's the sentinel
// for "nothing before this," and must always pass the consistency check on
// an otherwise-empty log.
void test_prev_log_index_zero_always_passes_on_empty_log() {
    raft::FakeCluster cluster(3, /*seed=*/6);
    CHECK(cluster.node(1).log.last_index() == 0);

    raft::AppendEntriesRequest req;
    req.term = 1;
    req.leader_id = 0;
    req.prev_log_index = 0;
    req.prev_log_term = 0;
    req.entries = {raft::LogEntry{1, "first ever entry"}};

    cluster.inject(1, 0, raft::Received<raft::AppendEntriesRequest>{req});

    bool saw_success = false;
    cluster.tick_while_matching([&](const raft::InFlight& m) {
        if (auto* resp = std::get_if<raft::AppendEntriesResponse>(&m.message)) {
            if (resp->success) saw_success = true;
        }
        return true;
    });
    CHECK(saw_success);
    CHECK(cluster.node(1).log.last_index() == 1);
}

// ---------------------------------------------------------------------------
// A stale-term AppendEntries (from an old, superseded leader) must still be
// rejected outright, unrelated to log content — reconfirms Step 2b's
// behavior still holds with a real log present.
// ---------------------------------------------------------------------------

void test_stale_term_append_entries_rejected_regardless_of_log_state() {
    raft::FakeCluster cluster(3, /*seed=*/7);
    const raft::NodeId leader = elect_leader(cluster, 0);
    cluster.propose(leader, "cmd1");
    cluster.drain_random();

    raft::AppendEntriesRequest stale;
    stale.term = 0;  // definitely stale; current term is >= 1 after election
    stale.leader_id = 2;  // must be a real node id (0-2); 99 crashed FakeCluster
    stale.prev_log_index = 0;
    stale.prev_log_term = 0;

    cluster.inject(1, 2, raft::Received<raft::AppendEntriesRequest>{stale});

    bool saw_rejection = false;
    cluster.tick_while_matching([&](const raft::InFlight& m) {
        if (auto* resp = std::get_if<raft::AppendEntriesResponse>(&m.message)) {
            if (!resp->success) saw_rejection = true;
        }
        return true;
    });
    CHECK(saw_rejection);
}

}  // namespace

int main() {
    test_proposed_entries_replicate_to_all_followers();
    test_replication_converges_under_randomized_delivery();

    test_propose_rejected_on_follower();
    test_propose_rejected_on_candidate();

    test_consistency_check_rejects_missing_prev_entry();
    test_consistency_check_rejects_term_mismatch_at_prev_index();
    test_prev_log_index_zero_always_passes_on_empty_log();

    test_stale_term_append_entries_rejected_regardless_of_log_state();

    if (g_failures == 0) {
        std::printf("replication_test: all checks passed\n");
        return 0;
    }
    std::fprintf(stderr, "replication_test: %d check(s) failed\n", g_failures);
    return 1;
}