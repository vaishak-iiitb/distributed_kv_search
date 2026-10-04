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
        const auto p1 = leader.ProposePut("x", "1");
        CHECK(p1.persisted && p1.result.accepted);
        const auto p2 = leader.ProposePut("y", "2");
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
        const auto p = nodes[*new_leader]->ProposePut("after_failover", "ok");
        CHECK(p.persisted && p.result.accepted);
    }

    for (std::size_t i = 0; i < nodes.size(); ++i) {
        if (ids[i] == *first_leader) continue;  // already stopped
        nodes[i]->Stop();
    }
    CleanNodeFiles(prefix, ids);
}

}  // namespace

int main() {
    test_three_node_cluster_elects_leader_and_replicates();
    test_cluster_recovers_after_leader_stops();

    if (g_failures == 0) {
        std::printf("raft_node_cluster_test: all checks passed\n");
        return 0;
    }
    std::fprintf(stderr, "raft_node_cluster_test: %d check(s) failed\n", g_failures);
    return 1;
}