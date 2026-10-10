// kv_metadata_server — one member of the metadata Raft group.
//
//   kv_metadata_server --id <n> --cluster <id=raft/client,id=raft/client,...>
//                      --data-dir <dir>
//                      [--election-min-ms <n>] [--election-max-ms <n>] [--heartbeat-ms <n>]
//
//   --cluster  every member of the group, THIS ONE INCLUDED, each as
//              <id>=<raft host:port>/<client host:port>. Pass the identical
//              string to every member; each picks its own ports by --id.
//   --id       which entry of --cluster this process is.
//
// The metadata group is an ordinary Raft group running the ordinary
// ClientServer with no key guard: it only ever stores the "shardmap" key.
// Unlike kv_shard_server it cannot learn its topology from the shard map
// (it IS where the shard map lives), hence the explicit --cluster.
//
//   Example (3 nodes on one machine):
//     C="0=127.0.0.1:7000/127.0.0.1:7100,1=127.0.0.1:7001/127.0.0.1:7101,2=127.0.0.1:7002/127.0.0.1:7102"
//     kv_metadata_server --id 0 --cluster "$C" --data-dir /tmp/meta &
//     kv_metadata_server --id 1 --cluster "$C" --data-dir /tmp/meta &
//     kv_metadata_server --id 2 --cluster "$C" --data-dir /tmp/meta &

#include <chrono>
#include <csignal>
#include <cstdint>
#include <filesystem>
#include <iostream>
#include <map>
#include <memory>
#include <string>
#include <thread>
#include <vector>

#include "raft/client_server.h"
#include "raft/raft_node.h"
#include "sharding/topology_text.h"

namespace {

volatile std::sig_atomic_t g_stop_requested = 0;

// Async-signal-safe: only sets a flag. The orderly Stop() runs on the main
// thread (same rule as kv_shard_server).
void HandleSignal(int) { g_stop_requested = 1; }

void PrintUsage() {
    std::cerr << "usage: kv_metadata_server --id <n> --cluster <id=raft_host:port/client_host:port,...> "
                 "--data-dir <dir> [--election-min-ms <n>] [--election-max-ms <n>] "
                 "[--heartbeat-ms <n>]\n";
}

bool ParseUnsigned(const std::string& text, std::uint64_t* out) {
    if (text.empty() || text.size() > 18) return false;
    std::uint64_t value = 0;
    for (char c : text) {
        if (c < '0' || c > '9') return false;
        value = value * 10 + static_cast<std::uint64_t>(c - '0');
    }
    *out = value;
    return true;
}

}  // namespace

int main(int argc, char** argv) {
    std::uint64_t self_id = 0;
    bool have_id = false;
    std::string cluster_text;
    std::string data_dir;
    std::uint64_t election_min = 150, election_max = 300, heartbeat = 50;

    for (int i = 1; i < argc; ++i) {
        const std::string flag = argv[i];
        if (i + 1 >= argc) {
            std::cerr << "missing value for " << flag << "\n";
            PrintUsage();
            return 2;
        }
        const std::string value = argv[++i];

        if (flag == "--id") {
            if (!ParseUnsigned(value, &self_id) || self_id > 0xFFFFFFFFull) {
                std::cerr << "bad --id\n";
                return 2;
            }
            have_id = true;
        } else if (flag == "--cluster") {
            cluster_text = value;
        } else if (flag == "--data-dir") {
            data_dir = value;
        } else if (flag == "--election-min-ms" || flag == "--election-max-ms" ||
                   flag == "--heartbeat-ms") {
            std::uint64_t n = 0;
            if (!ParseUnsigned(value, &n) || n == 0) {
                std::cerr << "bad " << flag << "\n";
                return 2;
            }
            if (flag == "--election-min-ms") election_min = n;
            else if (flag == "--election-max-ms") election_max = n;
            else heartbeat = n;
        } else {
            std::cerr << "unknown flag " << flag << "\n";
            PrintUsage();
            return 2;
        }
    }

    if (!have_id || cluster_text.empty() || data_dir.empty()) {
        PrintUsage();
        return 2;
    }
    if (election_min > election_max) {
        std::cerr << "--election-min-ms must not exceed --election-max-ms\n";
        return 2;
    }

    std::vector<sharding::ClusterNode> cluster;
    std::string err;
    if (!sharding::ParseClusterSpec(cluster_text, &cluster, &err)) {
        std::cerr << "bad --cluster: " << err << "\n";
        return 2;
    }

    // Split the cluster into "me" and "everyone else", for both port spaces.
    const sharding::ClusterNode* me = nullptr;
    raft::RaftNodeConfig raft_config;
    std::map<raft::NodeId, raft::PeerAddress> client_peers;
    for (const sharding::ClusterNode& node : cluster) {
        if (node.id == self_id) {
            me = &node;
            continue;
        }
        const auto peer_id = static_cast<raft::NodeId>(node.id);
        raft_config.peers[peer_id] = raft::PeerAddress{node.raft.host, node.raft.port};
        client_peers[peer_id] = raft::PeerAddress{node.client.host, node.client.port};
    }
    if (me == nullptr) {
        std::cerr << "--id " << self_id << " does not appear in --cluster\n";
        return 2;
    }

    std::error_code ec;
    std::filesystem::create_directories(data_dir, ec);
    if (ec) {
        std::cerr << "create_directories(" << data_dir << "): " << ec.message() << "\n";
        return 1;
    }
    const std::string base = data_dir + "/metadata_node" + std::to_string(self_id);

    raft_config.self_id = static_cast<raft::NodeId>(self_id);
    raft_config.listen_port = me->raft.port;
    raft_config.state_path = base + ".state";
    raft_config.log_path = base + ".log";
    raft_config.kv_data_dir = base + ".kv";
    raft_config.election_timeout_min = raft::Duration(election_min);
    raft_config.election_timeout_max = raft::Duration(election_max);
    raft_config.heartbeat_interval = raft::Duration(heartbeat);
    // Distinct per node so members do not time out in lockstep; never 0.
    raft_config.election_timer_seed = 1 + self_id;

    std::signal(SIGINT, HandleSignal);
    std::signal(SIGTERM, HandleSignal);

    // Declared raft_node first, client_server second: destroyed in reverse,
    // so the ClientServer (which holds a raw RaftNode*) dies first.
    auto raft_node = std::make_unique<raft::RaftNode>(raft_config);
    if (!raft_node->Start()) {
        std::cerr << "RaftNode::Start() failed (is port " << me->raft.port << " in use?)\n";
        return 1;
    }
    auto client_server =
        std::make_unique<raft::ClientServer>(me->client.port, raft_node.get(), std::move(client_peers));
    if (!client_server->Start()) {
        std::cerr << "ClientServer::Start() failed (is port " << me->client.port << " in use?)\n";
        raft_node->Stop();
        return 1;
    }

    std::cout << "metadata node " << self_id << " serving (raft " << me->raft.host << ":"
              << me->raft.port << ", client " << me->client.host << ":" << me->client.port
              << "; group of " << cluster.size() << "). Ctrl+C to stop.\n";

    while (!g_stop_requested) {
        std::this_thread::sleep_for(std::chrono::milliseconds(100));
    }

    std::cout << "stopping...\n";
    client_server->Stop();
    raft_node->Stop();
    return 0;
}