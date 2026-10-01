#include "raft/persistent_node.h"

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

void remove_if_exists(const std::string& path) { std::remove(path.c_str()); }

void test_fresh_node_has_default_state() {
    const std::string state_path = "pnode_test_fresh.state";
    const std::string log_path = "pnode_test_fresh.log";
    remove_if_exists(state_path);
    remove_if_exists(log_path);

    raft::PersistentRaftNode node(0, state_path, log_path, "pnode_test_fresh.kv");
    CHECK(node.state().current_term == 0);
    CHECK(!node.state().voted_for.has_value());
    CHECK(node.state().log.last_index() == 0);
}

// Bring node 0 to Leader in a 2-node scenario (self-vote + one external
// grant), which exercises both a term bump and a vote — then "restart"
// by constructing a SECOND PersistentRaftNode against the same files and
// confirm both survived.
void test_restart_recovers_term_and_vote() {
    const std::string state_path = "pnode_test_termvote.state";
    const std::string log_path = "pnode_test_termvote.log";
    remove_if_exists(state_path);
    remove_if_exists(log_path);

    raft::Term won_term = 0;
    {
        raft::PersistentRaftNode node(0, state_path, log_path, "pnode_test_termvote.kv");

        const auto r1 = node.Handle(0, raft::ElectionTimeout{}, 2);
        CHECK(r1.persisted);
        CHECK(node.state().role == raft::Role::kCandidate);

        raft::RequestVoteResponse granted;
        granted.term = node.state().current_term;
        granted.vote_granted = true;
        const auto r2 =
            node.Handle(1, raft::Received<raft::RequestVoteResponse>{granted}, 2);
        CHECK(r2.persisted);
        CHECK(node.state().role == raft::Role::kLeader);

        won_term = node.state().current_term;
    }  // node goes out of scope — files are already fsynced, nothing to close

    raft::PersistentRaftNode restarted(0, state_path, log_path, "pnode_test_termvote.kv");
    CHECK(restarted.state().current_term == won_term);
    CHECK(restarted.state().voted_for.has_value() &&
          *restarted.state().voted_for == 0);  // voted for self during election
    // Role is NOT persisted (design doc's own state split — see §2.2 of
    // the design doc: role is volatile, only term/vote/log are
    // persistent). A restarted node always starts as Follower and would
    // re-run an election if needed — that's correct Raft behavior, not a
    // gap in this step.
    CHECK(restarted.state().role == raft::Role::kFollower);

    remove_if_exists(state_path);
    remove_if_exists(log_path);
}

void test_restart_recovers_log_entries() {
    const std::string state_path = "pnode_test_log.state";
    const std::string log_path = "pnode_test_log.log";
    remove_if_exists(state_path);
    remove_if_exists(log_path);

    {
        raft::PersistentRaftNode node(0, state_path, log_path, "pnode_test_log.kv");
        node.Handle(0, raft::ElectionTimeout{}, 2);
        raft::RequestVoteResponse granted;
        granted.term = node.state().current_term;
        granted.vote_granted = true;
        node.Handle(1, raft::Received<raft::RequestVoteResponse>{granted}, 2);
        CHECK(node.state().role == raft::Role::kLeader);

        const auto p1 = node.Propose("cmd1");
        CHECK(p1.persisted && p1.result.accepted && p1.result.index == 1);
        const auto p2 = node.Propose("cmd2");
        CHECK(p2.persisted && p2.result.accepted && p2.result.index == 2);
    }

    raft::PersistentRaftNode restarted(0, state_path, log_path, "pnode_test_log.kv");
    CHECK(restarted.state().log.last_index() == 2);
    CHECK(restarted.state().log.at(1)->command == "cmd1");
    CHECK(restarted.state().log.at(2)->command == "cmd2");

    remove_if_exists(state_path);
    remove_if_exists(log_path);
}

// A Follower's log grows too (via AppendEntries acceptance), not just a
// Leader's via Propose — confirm that path persists correctly as well.
void test_follower_append_entries_persists_log() {
    const std::string state_path = "pnode_test_follower.state";
    const std::string log_path = "pnode_test_follower.log";
    remove_if_exists(state_path);
    remove_if_exists(log_path);

    {
        raft::PersistentRaftNode node(1, state_path, log_path, "pnode_test_follower.kv");

        raft::AppendEntriesRequest req;
        req.term = 1;
        req.leader_id = 0;
        req.prev_log_index = 0;
        req.prev_log_term = 0;
        req.entries = {raft::LogEntry{1, "replicated_cmd"}};

        const auto r = node.Handle(
            0, raft::Received<raft::AppendEntriesRequest>{req}, 3);
        CHECK(r.persisted);
        CHECK(node.state().log.last_index() == 1);
    }

    raft::PersistentRaftNode restarted(1, state_path, log_path, "pnode_test_follower.kv");
    CHECK(restarted.state().log.last_index() == 1);
    CHECK(restarted.state().log.at(1)->command == "replicated_cmd");
    CHECK(restarted.state().current_term == 1);  // adopted via maybe_update_term

    remove_if_exists(state_path);
    remove_if_exists(log_path);
}

// A failed persistence write must result in an EMPTY result, not the
// real one — proving the "don't send anything if the save failed"
// contract from design notes §3.3 is structurally enforced.
void test_persistence_failure_yields_empty_result() {
    // A path under a directory that doesn't exist: every Save/Append
    // against it will fail to open, deterministically.
    const std::string bad_state_path = "/nonexistent_dir_xyz/state.bin";
    const std::string bad_log_path = "/nonexistent_dir_xyz/log.bin";

    raft::PersistentRaftNode node(0, bad_state_path, bad_log_path, "pnode_test_badpath.kv");
    // Construction itself doesn't fail (Load()/Replay() on a missing
    // path just return "nothing there yet" — see design notes §2.1) —
    // the failure surfaces on the first attempted WRITE.
    const auto r = node.Handle(0, raft::ElectionTimeout{}, 3);

    CHECK(!r.persisted);
    CHECK(r.result.messages.empty());   // NOT the real broadcast — empty
    CHECK(!r.result.became_leader);
}
// A truncation must survive a restart just as faithfully as an ordinary
// append — proves PersistentRaftNode::Handle's Rewrite path (triggered by
// HandleResult::truncated_to) is actually wired up, not just present.
void test_restart_recovers_truncated_log() {
    const std::string state_path = "pnode_test_truncate.state";
    const std::string log_path = "pnode_test_truncate.log";
    remove_if_exists(state_path);
    remove_if_exists(log_path);

    {
        raft::PersistentRaftNode node(1, state_path, log_path, "pnode_test_truncate.kv");

        // Simulate a stale entry already on disk for this follower.
        raft::AppendEntriesRequest first;
        first.term = 1;
        first.leader_id = 0;
        first.prev_log_index = 0;
        first.prev_log_term = 0;
        first.entries = {raft::LogEntry{1, "stale"}};
        const auto r1 = node.Handle(0, raft::Received<raft::AppendEntriesRequest>{first}, 3);
        CHECK(r1.persisted);
        CHECK(node.state().log.last_index() == 1);

        // A new leader (higher term) overwrites it with a conflicting entry.
        raft::AppendEntriesRequest conflict;
        conflict.term = 2;
        conflict.leader_id = 2;
        conflict.prev_log_index = 0;
        conflict.prev_log_term = 0;
        conflict.entries = {raft::LogEntry{2, "correct"}};
        const auto r2 =
            node.Handle(2, raft::Received<raft::AppendEntriesRequest>{conflict}, 3);
        CHECK(r2.persisted);
        CHECK(r2.result.truncated_to.has_value());
        CHECK(node.state().log.at(1)->command == "correct");
    }

    raft::PersistentRaftNode restarted(1, state_path, log_path, "pnode_test_truncate.kv");
    CHECK(restarted.state().log.last_index() == 1);
    CHECK(restarted.state().log.at(1)->term == 2);
    CHECK(restarted.state().log.at(1)->command == "correct");  // NOT "stale"

    remove_if_exists(state_path);
    remove_if_exists(log_path);
}

}  // namespace

int main() {
    test_fresh_node_has_default_state();
    test_restart_recovers_term_and_vote();
    test_restart_recovers_log_entries();
    test_follower_append_entries_persists_log();
    test_persistence_failure_yields_empty_result();
    test_persistence_failure_yields_empty_result();
    test_restart_recovers_truncated_log();

    if (g_failures == 0) {
        std::printf("persistent_node_test: all checks passed\n");
        return 0;
    }
    std::fprintf(stderr, "persistent_node_test: %d check(s) failed\n", g_failures);
    return 1;
}