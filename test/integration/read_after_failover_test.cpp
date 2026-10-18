// Regression test for flag #2: "a client that was told OK can read its own
// write after a leader change".
//
// Three real RaftNodes + three ClientServers in ONE process (real sockets).
// Per trial: acknowledged Put, then the leader is stopped immediately
// (ClientServer first, so clients are refused at once; then the RaftNode),
// then Get through RaftGroupClient. Heartbeats are slow (200 ms) so the
// window "entry acknowledged, followers do not yet know it is committed" is
// wide, and the Get lands on a NEW leader.
//
// Before the fix (no leader no-op, no read barrier) the new leader answered
// NOT_FOUND. The multi-process version of this check (SIGKILL) is
// multi_shard_test's write-then-kill phase; this test needs no binaries.
//
// Note: stopping a node cooperatively is not a crash. A heartbeat can slip out
// during the few milliseconds Stop() takes, which would let a pre-fix server
// pass by luck; the test can therefore pass spuriously, never fail spuriously.

#include <unistd.h>

#include <chrono>
#include <cstdio>
#include <filesystem>
#include <map>
#include <memory>
#include <string>
#include <thread>
#include <vector>

#include "net/line_client.h"
#include "raft/client_server.h"
#include "raft/raft_node.h"
#include "sharding/raft_group_client.h"

namespace {

using namespace std::chrono_literals;
namespace fs = std::filesystem;

int g_failures = 0;
#define CHECK(cond)                                                                       \
    do {                                                                                  \
        if (!(cond)) {                                                                    \
            std::fprintf(stderr, "%s:%d: CHECK failed: %s\n", __FILE__, __LINE__, #cond); \
            ++g_failures;                                                                 \
        }                                                                                 \
    } while (0)

constexpr int kNodes = 3;
constexpr int kTrials = 3;
constexpr int kPortBase = 33000;  // + trial * 20 + {0..2} raft, {10..12} client

struct Node {
    int raft_port = 0, client_port = 0;
    std::unique_ptr<raft::RaftNode> raft;
    std::unique_ptr<raft::ClientServer> server;
};

int FindLeader(const std::vector<Node>& nodes, std::chrono::milliseconds timeout) {
    const auto deadline = std::chrono::steady_clock::now() + timeout;
    while (std::chrono::steady_clock::now() < deadline) {
        for (int i = 0; i < kNodes; ++i) {
            if (!nodes[i].server) continue;
            const auto r = net::LineClient::RoundTrip("127.0.0.1", nodes[i].client_port, "LEADER", 200);
            if (r.has_value() && *r == "LEADER") return i;
        }
        std::this_thread::sleep_for(100ms);
    }
    return -1;
}

void RunTrial(int trial, const std::string& dir) {
    std::vector<Node> nodes(kNodes);
    for (int i = 0; i < kNodes; ++i) {
        nodes[i].raft_port = kPortBase + trial * 20 + i;
        nodes[i].client_port = kPortBase + trial * 20 + 10 + i;
    }
    for (int i = 0; i < kNodes; ++i) {
        raft::RaftNodeConfig c;
        c.self_id = static_cast<raft::NodeId>(i);
        c.listen_port = nodes[i].raft_port;
        c.state_path = dir + "/t" + std::to_string(trial) + "_n" + std::to_string(i) + ".state";
        c.log_path = dir + "/t" + std::to_string(trial) + "_n" + std::to_string(i) + ".log";
        c.kv_data_dir = dir + "/t" + std::to_string(trial) + "_n" + std::to_string(i) + ".kv";
        c.election_timeout_min = raft::Duration(500);
        c.election_timeout_max = raft::Duration(800);
        c.heartbeat_interval = raft::Duration(200);
        c.election_timer_seed = 1000 * (trial + 1) + i + 1;
        std::map<raft::NodeId, raft::PeerAddress> client_peers;
        for (int j = 0; j < kNodes; ++j) {
            if (j == i) continue;
            c.peers[static_cast<raft::NodeId>(j)] = raft::PeerAddress{"127.0.0.1", nodes[j].raft_port};
            client_peers[static_cast<raft::NodeId>(j)] = raft::PeerAddress{"127.0.0.1", nodes[j].client_port};
        }
        nodes[i].raft = std::make_unique<raft::RaftNode>(c);
        CHECK(nodes[i].raft->Start());
        nodes[i].server = std::make_unique<raft::ClientServer>(nodes[i].client_port, nodes[i].raft.get(),
                                                               client_peers);
        CHECK(nodes[i].server->Start());
    }

    std::vector<sharding::Endpoint> endpoints;
    for (const auto& n : nodes) endpoints.push_back({"127.0.0.1", n.client_port});
    sharding::RaftGroupClientOptions options;
    options.overall_timeout_ms = 15000;
    sharding::RaftGroupClient client(endpoints, options);

    const int leader = FindLeader(nodes, 10s);
    CHECK(leader >= 0);
    if (leader < 0) return;
    std::this_thread::sleep_for(700ms);  // settle: followers know the leader and the commit index

    const std::string key = "k-" + std::to_string(trial);
    const auto put = client.Put(key, "v");
    CHECK(put.status == sharding::RaftGroupClient::Status::kOk);

    // Stop the leader at once: refuse clients first, then stop Raft.
    nodes[leader].server->Stop();
    nodes[leader].raft->Stop();

    const auto got = client.Get(key);
    if (got.status != sharding::RaftGroupClient::Status::kOk || got.value != "v") {
        std::fprintf(stderr,
                     "trial %d: key %s was acknowledged, leader (node %d) stopped at once, Get -> "
                     "status %d value '%s' (%s)\n",
                     trial, key.c_str(), leader, static_cast<int>(got.status), got.value.c_str(),
                     got.detail.c_str());
    }
    CHECK(got.status == sharding::RaftGroupClient::Status::kOk && got.value == "v");

    // The new leader must also keep working normally.
    CHECK(client.Put(key + "-after", "w").status == sharding::RaftGroupClient::Status::kOk);
    const auto after = client.Get(key + "-after");
    CHECK(after.status == sharding::RaftGroupClient::Status::kOk && after.value == "w");

    for (auto& n : nodes) {
        if (n.server) n.server->Stop();
        if (n.raft) n.raft->Stop();
    }
}

}  // namespace

int main() {
    const std::string dir =
        (fs::temp_directory_path() / ("read_after_failover_test_" + std::to_string(::getpid()))).string();
    std::error_code ec;
    fs::remove_all(dir, ec);
    fs::create_directories(dir, ec);

    for (int trial = 0; trial < kTrials; ++trial) RunTrial(trial, dir);

    fs::remove_all(dir, ec);
    if (g_failures == 0) {
        std::printf("read_after_failover_test: all checks passed\n");
        return 0;
    }
    std::fprintf(stderr, "read_after_failover_test: %d check(s) failed\n", g_failures);
    return 1;
}