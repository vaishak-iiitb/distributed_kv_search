#include "raft/raft_node.h"

#include <chrono>
#include <cstdio>
#include <functional>
#include <memory>
#include <optional>
#include <thread>
#include <vector>

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

bool WaitUntil(const std::function<bool()>& predicate, std::chrono::milliseconds timeout,
              std::chrono::milliseconds poll = std::chrono::milliseconds(20)) {
    const auto deadline = std::chrono::steady_clock::now() + timeout;
    while (std::chrono::steady_clock::now() < deadline) {
        if (predicate()) return true;
        std::this_thread::sleep_for(poll);
    }
    return predicate();
}

// Short but safe timings: 100-200ms election range, 30ms heartbeat —
// comfortably inside validate_timing_config's 2x margin (Step 3a),
// while keeping this test suite fast.
raft::RaftNodeConfig MakeConfig(raft::NodeId id, int base_port,
                                const std::vector<raft::NodeId>& all_ids,
                                const std::string& prefix) {
    raft::RaftNodeConfig cfg;
    cfg.self_id = id;
    cfg.listen_port = base_port + static_cast<int>(id);
    for (raft::NodeId peer : all_ids) {
        if (peer == id) continue;
        cfg.peers[peer] = raft::PeerAddress{"127.0.0.1", base_port + static_cast<int>(peer)};
    }
    cfg.state_path = prefix + "_n" + std::to_string(id) + ".state";
    cfg.log_path = prefix + "_n" + std::to_string(id) + ".log";
    cfg.kv_data_dir = prefix + "_n" + std::to_string(id) + ".kv";
    cfg.election_timeout_min = raft::Duration(100);
    cfg.election_timeout_max = raft::Duration(200);
    cfg.heartbeat_interval = raft::Duration(30);
    cfg.election_timer_seed = 1000 + id;
    return cfg;
}

void CleanNodeFiles(const std::string& prefix, const std::vector<raft::NodeId>& ids) {
    for (raft::NodeId id : ids) {
        remove_if_exists(prefix + "_n" + std::to_string(id) + ".state");
        remove_if_exists(prefix + "_n" + std::to_string(id) + ".log");
    }
}

std::optional<raft::NodeId> FindLeader(std::vector<std::unique_ptr<raft::RaftNode>>& nodes,
                                       const std::vector<raft::NodeId>& ids) {
    for (std::size_t i = 0; i < nodes.size(); ++i) {
        if (nodes[i]->CurrentRole() == raft::Role::kLeader) return ids[i];
    }
    return std::nullopt;
}

// ---------------------------------------------------------------------------

void test_three_node_cluster_elects_leader_and_replicates() {
    const std::vector<raft::NodeId> ids = {0, 1, 2};
    const std::string prefix = "raft_node_test_basic";
    CleanNodeFiles(prefix, ids);
    const int base_port = 18100;

    std::vector<std::unique_ptr<raft::RaftNode>> nodes;
    for (raft::NodeId id : ids) {
        nodes.push_back(
            std::make_unique<raft::RaftNode>(MakeConfig(id, base_port, ids, prefix)));
        CHECK(nodes.back()->Start());
    }

    CHECK(WaitUntil([&] { return FindLeader(nodes, ids).has_value(); },
                    std::chrono::milliseconds(3000)));
    const auto leader_id = FindLeader(nodes, ids);
    CHECK(leader_id.has_value());

    if (leader_id) {
        raft::RaftNode& leader = *nodes[*leader_id];
        const auto p1 = leader.ProposePut(/*client_id=*/1, /*sequence_num=*/1, "x", "1");
        CHECK(p1.persisted && p1.result.accepted);
        const auto p2 = leader.ProposePut(/*client_id=*/2, /*sequence_num=*/1, "y", "2");
        CHECK(p2.persisted && p2.result.accepted);

        for (std::size_t i = 0; i < nodes.size(); ++i) {
            std::string value;
            CHECK(WaitUntil([&] { return nodes[i]->Get("x", &value) && value == "1"; },
                            std::chrono::milliseconds(3000)));
        }
    }

    for (auto& node : nodes) node->Stop();
    CleanNodeFiles(prefix, ids);
}

// The flagship plan's own milestone, literally: kill the leader
// mid-operation, confirm a new leader is elected and the cluster keeps
// accepting writes.
void test_cluster_recovers_after_leader_stops() {
    const std::vector<raft::NodeId> ids = {0, 1, 2};
    const std::string prefix = "raft_node_test_recover";
    CleanNodeFiles(prefix, ids);
    const int base_port = 18200;

    std::vector<std::unique_ptr<raft::RaftNode>> nodes;
    for (raft::NodeId id : ids) {
        nodes.push_back(
            std::make_unique<raft::RaftNode>(MakeConfig(id, base_port, ids, prefix)));
        CHECK(nodes.back()->Start());
    }

    CHECK(WaitUntil([&] { return FindLeader(nodes, ids).has_value(); },
                    std::chrono::milliseconds(3000)));
    const auto first_leader = FindLeader(nodes, ids);
    CHECK(first_leader.has_value());
    if (!first_leader) return;

    nodes[*first_leader]->Stop();  // the leader "dies"

    std::optional<raft::NodeId> new_leader;
    CHECK(WaitUntil(
        [&] {
            for (std::size_t i = 0; i < nodes.size(); ++i) {
                if (ids[i] == *first_leader) continue;
                if (nodes[i]->CurrentRole() == raft::Role::kLeader) {
                    new_leader = ids[i];
                    return true;
                }
            }
            return false;
        },
        std::chrono::milliseconds(3000)));

    CHECK(new_leader.has_value());
    CHECK(new_leader.has_value() && *new_leader != *first_leader);

    if (new_leader) {
        const auto p = nodes[*new_leader]->ProposePut(/*client_id=*/1, /*sequence_num=*/1, "after_failover", "ok");
        CHECK(p.persisted && p.result.accepted);
    }

    for (std::size_t i = 0; i < nodes.size(); ++i) {
        if (ids[i] == *first_leader) continue;  // already stopped
        nodes[i]->Stop();
    }
    CleanNodeFiles(prefix, ids);
}

}  // namespace

void test_non_leader_reports_correct_leader_hint() {
    const std::vector<raft::NodeId> ids = {0, 1, 2};
    const std::string prefix = "raft_node_test_redirect";
    CleanNodeFiles(prefix, ids);
    const int base_port = 18300;

    std::vector<std::unique_ptr<raft::RaftNode>> nodes;
    for (raft::NodeId id : ids) {
        nodes.push_back(
            std::make_unique<raft::RaftNode>(MakeConfig(id, base_port, ids, prefix)));
        CHECK(nodes.back()->Start());
    }

    CHECK(WaitUntil([&] { return FindLeader(nodes, ids).has_value(); },
                    std::chrono::milliseconds(3000)));
    const auto leader_id = FindLeader(nodes, ids);
    CHECK(leader_id.has_value());
    if (!leader_id) return;

    // A follower only records the leader hint once it has actually
    // ACCEPTED an AppendEntries (design notes §2.1) — not merely once
    // the election concluded — so give the first heartbeat a moment.
    CHECK(WaitUntil(
        [&] {
            for (std::size_t i = 0; i < nodes.size(); ++i) {
                if (ids[i] == *leader_id) continue;
                const auto hint = nodes[i]->KnownLeaderHint();
                if (!hint.leader_id.has_value() || *hint.leader_id != *leader_id) return false;
            }
            return true;
        },
        std::chrono::milliseconds(2000)));

    for (std::size_t i = 0; i < nodes.size(); ++i) {
        if (ids[i] == *leader_id) continue;
        raft::RaftNode& follower = *nodes[i];

        const auto p = follower.ProposePut(/*client_id=*/1, /*sequence_num=*/1, "x", "1");
        CHECK(p.persisted);
        CHECK(!p.result.accepted);  // this node isn't leader

        const auto hint = follower.KnownLeaderHint();
        CHECK(hint.leader_id.has_value() && *hint.leader_id == *leader_id);
        CHECK(hint.address.has_value());
        CHECK(hint.address->port == base_port + static_cast<int>(*leader_id));
    }

    for (auto& node : nodes) node->Stop();
    CleanNodeFiles(prefix, ids);
}
void test_propose_and_wait_confirms_real_commit() {
    const std::vector<raft::NodeId> ids = {0, 1, 2};
    const std::string prefix = "raft_node_test_wait";
    CleanNodeFiles(prefix, ids);
    const int base_port = 18400;

    std::vector<std::unique_ptr<raft::RaftNode>> nodes;
    for (raft::NodeId id : ids) {
        nodes.push_back(
            std::make_unique<raft::RaftNode>(MakeConfig(id, base_port, ids, prefix)));
        CHECK(nodes.back()->Start());
    }

    CHECK(WaitUntil([&] { return FindLeader(nodes, ids).has_value(); },
                    std::chrono::milliseconds(3000)));
    const auto leader_id = FindLeader(nodes, ids);
    CHECK(leader_id.has_value());
    if (!leader_id) return;

    const auto result = nodes[*leader_id]->ProposePutAndWait(
        /*client_id=*/1, /*sequence_num=*/1, "x", "1", raft::Duration(2000));

    CHECK(result.durable.persisted);
    CHECK(result.durable.result.accepted);
    CHECK(result.committed);
    CHECK(!result.timed_out);
    CHECK(!result.lost_leadership);

    // Having returned "committed", the write must actually be durable
    // on the leader right now — no further waiting needed.
    std::string value;
    CHECK(nodes[*leader_id]->Get("x", &value));
    CHECK(value == "1");

    for (auto& node : nodes) node->Stop();
    CleanNodeFiles(prefix, ids);
}

// A write proposed to a node that then immediately stops (simulating a
// crash right after proposing, before any peer could possibly ack) must
// report an honest, ambiguous outcome — never a false "committed".
void test_propose_and_wait_reports_ambiguous_outcome_on_stop() {
    const std::vector<raft::NodeId> ids = {0, 1, 2};
    const std::string prefix = "raft_node_test_wait_stop";
    CleanNodeFiles(prefix, ids);
    const int base_port = 18500;

    std::vector<std::unique_ptr<raft::RaftNode>> nodes;
    for (raft::NodeId id : ids) {
        nodes.push_back(
            std::make_unique<raft::RaftNode>(MakeConfig(id, base_port, ids, prefix)));
        CHECK(nodes.back()->Start());
    }

    CHECK(WaitUntil([&] { return FindLeader(nodes, ids).has_value(); },
                    std::chrono::milliseconds(3000)));
    const auto leader_id = FindLeader(nodes, ids);
    CHECK(leader_id.has_value());
    if (!leader_id) return;

    // Stop every OTHER node first, so the leader has no one left to
    // reach majority with — its wait can only time out or notice it's
    // lost leadership, never confirm a real commit.
    for (std::size_t i = 0; i < nodes.size(); ++i) {
        if (ids[i] == *leader_id) continue;
        nodes[i]->Stop();
    }

    const auto result = nodes[*leader_id]->ProposePutAndWait(
        /*client_id=*/2, /*sequence_num=*/1, "y", "2", raft::Duration(500));

    CHECK(result.durable.persisted);
    CHECK(result.durable.result.accepted);  // it WAS proposed — this node was leader
    CHECK(!result.committed);               // but never confirmed as committed
    CHECK(result.timed_out || result.lost_leadership);  // one honest, ambiguous outcome

    nodes[*leader_id]->Stop();
    CleanNodeFiles(prefix, ids);
}

int main() {
    test_three_node_cluster_elects_leader_and_replicates();
    test_cluster_recovers_after_leader_stops();
    test_non_leader_reports_correct_leader_hint();
    test_propose_and_wait_confirms_real_commit();
    test_propose_and_wait_reports_ambiguous_outcome_on_stop();
    if (g_failures == 0) {
        std::printf("raft_node_cluster_test: all checks passed\n");
        return 0;
    }
    std::fprintf(stderr, "raft_node_cluster_test: %d check(s) failed\n", g_failures);
    return 1;
}