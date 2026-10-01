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

void clean_node_files(const std::string& prefix) {
    remove_if_exists(prefix + ".state");
    remove_if_exists(prefix + ".log");
    // kv_data_dir is a directory KVEngine manages; leftover files from a
    // prior run are tolerated (KVEngine recovers whatever's there), so
    // no directory cleanup here beyond what each test needs explicitly.
}

// Elects `leader` (id 0) with `follower` (id 1) as its only real peer, in
// a 3-node cluster_size (majority=2). Hand-relays every message — there
// is no FakeCluster here, since persistence isn't wired into it.
void elect_two_node_leader(raft::PersistentRaftNode& leader,
                           raft::PersistentRaftNode& follower) {
    const auto r1 = leader.Handle(0, raft::ElectionTimeout{}, 3);
    CHECK(r1.persisted);
    const auto* vote_req =
        std::get_if<raft::RequestVoteRequest>(&r1.result.messages[0].message);
    CHECK(vote_req != nullptr);

    const auto r2 =
        follower.Handle(0, raft::Received<raft::RequestVoteRequest>{*vote_req}, 3);
    CHECK(r2.persisted);
    const auto* vote_resp =
        std::get_if<raft::RequestVoteResponse>(&r2.result.messages[0].message);
    CHECK(vote_resp != nullptr && vote_resp->vote_granted);

    const auto r3 =
        leader.Handle(1, raft::Received<raft::RequestVoteResponse>{*vote_resp}, 3);
    CHECK(r3.persisted);
    CHECK(r3.result.became_leader);

    // Assert-leadership heartbeat and its response — this is the
    // "first contact" exchange that lazily seeds next_index/match_index
    // (Step 6 design notes §3.2); its own success/failure is discarded.
    const auto* heartbeat =
        std::get_if<raft::AppendEntriesRequest>(&r3.result.messages[0].message);
    CHECK(heartbeat != nullptr);
    const auto r4 =
        follower.Handle(0, raft::Received<raft::AppendEntriesRequest>{*heartbeat}, 3);
    CHECK(r4.persisted);
    const auto* hb_resp =
        std::get_if<raft::AppendEntriesResponse>(&r4.result.messages[0].message);
    CHECK(hb_resp != nullptr);
    leader.Handle(1, raft::Received<raft::AppendEntriesResponse>{*hb_resp}, 3);
}

// Relays exactly one AppendEntriesRequest (from `from_result`) to
// `receiver`, then relays its response back to `sender`. Returns the
// sender-side DurableResult from processing that response (this is
// where commit/apply, if any, actually happens on the sender/leader).
raft::PersistentRaftNode::DurableResult relay_append_entries(
    const raft::HandleResult& from_result, raft::PersistentRaftNode& sender,
    raft::PersistentRaftNode& receiver) {
    const auto* req =
        std::get_if<raft::AppendEntriesRequest>(&from_result.messages[0].message);
    CHECK(req != nullptr);

    const auto recv_result =
        receiver.Handle(0, raft::Received<raft::AppendEntriesRequest>{*req}, 3);
    CHECK(recv_result.persisted);

    const auto* resp =
        std::get_if<raft::AppendEntriesResponse>(&recv_result.result.messages[0].message);
    CHECK(resp != nullptr);

    return sender.Handle(1, raft::Received<raft::AppendEntriesResponse>{*resp}, 3);
}

// ---------------------------------------------------------------------------

void test_committed_put_is_applied_and_readable_on_leader() {
    const std::string prefix = "apply_test_put";
    clean_node_files(prefix + "_n0");
    clean_node_files(prefix + "_n1");

    raft::PersistentRaftNode leader(0, prefix + "_n0.state", prefix + "_n0.log",
                                    prefix + "_n0.kv");
    raft::PersistentRaftNode follower(1, prefix + "_n1.state", prefix + "_n1.log",
                                      prefix + "_n1.kv");
    elect_two_node_leader(leader, follower);

    const auto propose_result = leader.ProposePut("x", "1");
    CHECK(propose_result.persisted && propose_result.result.accepted);

    const auto handle_result =
        relay_append_entries(propose_result.result.to_send, leader, follower);

    // Majority(3)=2: leader's own tip + this one follower's ack is
    // enough — commits and applies right here, on the leader.
    CHECK(handle_result.result.to_apply.size() == 1);
    CHECK(leader.state().commit_index == 1);

    std::string value;
    CHECK(leader.Get("x", &value));
    CHECK(value == "1");

    // One-round lag (Step 7a): the follower hasn't been told leader_commit
    // advanced yet — no message carrying it has gone out since it changed.
    CHECK(!follower.Get("x", &value));

    // A second write carries leader_commit=1 to the follower this time.
    const auto propose2 = leader.ProposePut("y", "2");
    relay_append_entries(propose2.result.to_send, leader, follower);

    CHECK(follower.Get("x", &value));  // NOW visible
    CHECK(value == "1");
}

void test_delete_removes_value_once_applied() {
    const std::string prefix = "apply_test_delete";
    clean_node_files(prefix + "_n0");
    clean_node_files(prefix + "_n1");

    raft::PersistentRaftNode leader(0, prefix + "_n0.state", prefix + "_n0.log",
                                    prefix + "_n0.kv");
    raft::PersistentRaftNode follower(1, prefix + "_n1.state", prefix + "_n1.log",
                                      prefix + "_n1.kv");
    elect_two_node_leader(leader, follower);

    const auto put_result = leader.ProposePut("k", "v");
    relay_append_entries(put_result.result.to_send, leader, follower);

    std::string value;
    CHECK(leader.Get("k", &value));
    CHECK(value == "v");

    const auto delete_result = leader.ProposeDelete("k");
    relay_append_entries(delete_result.result.to_send, leader, follower);

    CHECK(!leader.Get("k", &value));  // gone, once applied
}

// KVEngine's own Phase 1 durability means the applied value survives
// independently of Raft's (unpersisted) commit_index/last_applied — see
// design notes §2.2.
void test_applied_data_survives_restart_independently_of_raft_state() {
    const std::string prefix = "apply_test_restart";
    clean_node_files(prefix + "_n0");
    clean_node_files(prefix + "_n1");

    {
        raft::PersistentRaftNode leader(0, prefix + "_n0.state", prefix + "_n0.log",
                                        prefix + "_n0.kv");
        raft::PersistentRaftNode follower(1, prefix + "_n1.state", prefix + "_n1.log",
                                          prefix + "_n1.kv");
        elect_two_node_leader(leader, follower);

        const auto put_result = leader.ProposePut("durable_key", "durable_value");
        relay_append_entries(put_result.result.to_send, leader, follower);

        std::string value;
        CHECK(leader.Get("durable_key", &value));
        CHECK(value == "durable_value");
    }  // leader destroyed — Raft-side in-memory state is gone

    raft::PersistentRaftNode restarted(0, prefix + "_n0.state", prefix + "_n0.log",
                                       prefix + "_n0.kv");
    CHECK(restarted.state().commit_index == 0);  // volatile — correctly reset

    std::string value;
    CHECK(restarted.Get("durable_key", &value));  // but the DATA survived
    CHECK(value == "durable_value");
}

}  // namespace

int main() {
    test_committed_put_is_applied_and_readable_on_leader();
    test_delete_removes_value_once_applied();
    test_applied_data_survives_restart_independently_of_raft_state();

    if (g_failures == 0) {
        std::printf("apply_test: all checks passed\n");
        return 0;
    }
    std::fprintf(stderr, "apply_test: %d check(s) failed\n", g_failures);
    return 1;
}