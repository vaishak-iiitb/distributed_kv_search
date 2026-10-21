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
// The Figure 8 scenario itself (design doc §6.2, design notes §2.1),
// reconstructed by hand against a single RaftState — no cluster needed to
// isolate exactly what the commit rule does and doesn't allow.
// ---------------------------------------------------------------------------

void test_old_term_entry_not_committed_by_majority_count_alone() {
    raft::RaftState leader;
    leader.self_id = 0;
    leader.role = raft::Role::kLeader;
    leader.current_term = 4;               // this leader won term 4
    leader.log.append(1, "idx1");          // term 1
    leader.log.append(2, "idx2_old_term"); // term 2 — the "risky" entry
    leader.commit_index = 0;
    leader.last_applied = 0;

    // Pre-seed tracking as if peer 1 is about to have its index-2
    // replication acknowledged, and peer 2 is still behind. (How they
    // GOT here is backtrack_test.cpp's concern; this test isolates the
    // commit rule alone.)
    leader.next_index[1] = 2;
    leader.match_index[1] = 1;
    leader.next_index[2] = 1;
    leader.match_index[2] = 0;

    raft::AppendEntriesResponse resp;
    resp.term = 4;
    resp.success = true;
    resp.match_index = 2;  // the follower says it matched through index 2
    const raft::HandleResult r =
        raft::handle(leader, 1, raft::Received<raft::AppendEntriesResponse>{resp}, 3);

    CHECK(leader.match_index[1] == 2);  // tracking itself still updates normally
    // positions: [leader=2, peer1=2, peer2=0] -> majority(3)=2 -> candidate=2
    // log.term_at(2) == 2, but current_term == 4 -> MUST NOT commit.
    CHECK(leader.commit_index == 0);
    CHECK(r.to_apply.empty());
}

void test_new_term_entry_commits_and_carries_old_entry_with_it() {
    raft::RaftState leader;
    leader.self_id = 0;
    leader.role = raft::Role::kLeader;
    leader.current_term = 4;
    leader.log.append(1, "idx1");
    leader.log.append(2, "idx2_old_term");
    leader.commit_index = 0;
    leader.last_applied = 0;
    leader.next_index[1] = 3;  // already past idx2, as if the previous
    leader.match_index[1] = 2; // test's exchange already happened
    leader.next_index[2] = 1;
    leader.match_index[2] = 0;

    const raft::ProposeResult p = raft::propose(leader, "idx3_new_term");
    CHECK(p.accepted && p.index == 3);
    CHECK(leader.log.term_at(3) == 4);  // written in the CURRENT term

    raft::AppendEntriesResponse resp;
    resp.term = 4;
    resp.success = true;
    resp.match_index = 3;  // the follower says it matched through index 3
    const raft::HandleResult r =
        raft::handle(leader, 1, raft::Received<raft::AppendEntriesResponse>{resp}, 3);

    // positions: [leader=3, peer1=3, peer2=0] -> majority(3)=2 -> candidate=3
    // log.term_at(3) == current_term(4) -> COMMITS.
    CHECK(leader.commit_index == 3);
    // Applying catches up from last_applied(0) all the way to 3 — the
    // old term-2 entry rides along, exactly as design doc §6.2 describes.
    CHECK(r.to_apply.size() == 3);
    CHECK(r.to_apply[0].command == "idx1");
    CHECK(r.to_apply[1].command == "idx2_old_term");
    CHECK(r.to_apply[2].command == "idx3_new_term");
}

// ---------------------------------------------------------------------------
// Majority arithmetic with a mix of tracked and untracked peers.
// ---------------------------------------------------------------------------

void test_untracked_peers_count_as_zero_not_ignored() {
    raft::RaftState leader;
    leader.self_id = 0;
    leader.role = raft::Role::kLeader;
    leader.current_term = 1;
    leader.log.append(1, "a");
    leader.log.append(1, "b");
    leader.commit_index = 0;

    // 5-node cluster. Only ONE peer tracked and caught up; the other
    // three are untracked (never contacted yet). Majority(5)=3.
    leader.next_index[1] = 3;
    leader.match_index[1] = 2;

    raft::AppendEntriesResponse resp;
    resp.term = 1;
    resp.success = true;
    resp.match_index = 2;  // the follower says it matched through index 2
    const raft::HandleResult r =
        raft::handle(leader, 1, raft::Received<raft::AppendEntriesResponse>{resp}, 5);

    // positions: [leader=2, peer1=2, 0, 0, 0] sorted desc [2,2,0,0,0]
    // majority(5)=3 -> candidate = positions[2] = 0 -> nothing commits.
    // If untracked peers were silently ignored instead of counted as 0,
    // this would incorrectly compute majority over only 2 known nodes
    // and commit index 2 — exactly the bug this test exists to catch.
    CHECK(leader.commit_index == 0);
    CHECK(r.to_apply.empty());
}

// ---------------------------------------------------------------------------
// Follower-side adoption, bounded by what was actually delivered.
// ---------------------------------------------------------------------------

void test_follower_adopts_leader_commit_bounded_by_delivered_entries() {
    raft::RaftState follower;
    follower.self_id = 1;
    follower.commit_index = 0;

    raft::AppendEntriesRequest req;
    req.term = 1;
    req.leader_id = 0;
    req.prev_log_index = 0;
    req.prev_log_term = 0;
    req.entries = {raft::LogEntry{1, "a"}, raft::LogEntry{1, "b"}};
    req.leader_commit = 10;  // claims far more committed than this message delivers

    const raft::HandleResult r =
        raft::handle(follower, 0, raft::Received<raft::AppendEntriesRequest>{req}, 3);

    // last_new_entry_index = prev(0) + entries(2) = 2. min(10, 2) = 2.
    CHECK(follower.commit_index == 2);
    CHECK(r.to_apply.size() == 2);
    CHECK(r.to_apply[0].command == "a");
    CHECK(r.to_apply[1].command == "b");
}

void test_follower_commit_never_decreases() {
    raft::RaftState follower;
    follower.self_id = 1;
    follower.log.append(1, "a");
    follower.commit_index = 1;
    follower.last_applied = 1;

    raft::AppendEntriesRequest req;  // a stale/duplicate heartbeat with a
    req.term = 1;                    // lower leaderCommit than we already have
    req.leader_id = 0;
    req.prev_log_index = 1;
    req.prev_log_term = 1;
    req.leader_commit = 0;

    const raft::HandleResult r =
        raft::handle(follower, 0, raft::Received<raft::AppendEntriesRequest>{req}, 3);

    CHECK(follower.commit_index == 1);  // unchanged, not regressed to 0
    CHECK(r.to_apply.empty());
}

// ---------------------------------------------------------------------------
// End-to-end via FakeCluster: commit_index actually converges across a
// real cluster after real proposes and drains.
// ---------------------------------------------------------------------------

void test_commit_index_converges_across_cluster() {
    raft::FakeCluster cluster(3, /*seed=*/21);
    const raft::NodeId leader = elect_leader(cluster, 0);

    cluster.propose(leader, "cmd1");
    cluster.drain_random();
    cluster.propose(leader, "cmd2");
    cluster.drain_random();

    CHECK(cluster.node(leader).commit_index == 2);
    CHECK(cluster.node(leader).last_applied == 2);

    // leaderCommit only propagates by riding on the NEXT AppendEntries
    // (design doc §5.3 step 6) — there's no dedicated announcement
    // message, and without periodic heartbeats (Step 8) a follower's
    // knowledge of commit_index can legitimately lag one round behind
    // the leader's own. A third propose is what finally carries
    // leader_commit=2 (the value as of cmd2's commit) to the followers.
    cluster.propose(leader, "cmd3");
    cluster.drain_random();

    CHECK(cluster.node(leader).commit_index == 3);
    CHECK(cluster.node(1).commit_index == 2);
    CHECK(cluster.node(2).commit_index == 2);
}

}  // namespace

int main() {
    test_old_term_entry_not_committed_by_majority_count_alone();
    test_new_term_entry_commits_and_carries_old_entry_with_it();

    test_untracked_peers_count_as_zero_not_ignored();

    test_follower_adopts_leader_commit_bounded_by_delivered_entries();
    test_follower_commit_never_decreases();

    test_commit_index_converges_across_cluster();

    if (g_failures == 0) {
        std::printf("commit_test: all checks passed\n");
        return 0;
    }
    std::fprintf(stderr, "commit_test: %d check(s) failed\n", g_failures);
    return 1;
}