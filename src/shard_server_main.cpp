// kv_shard_server — one member of one data shard.
//
//   kv_shard_server --shard <id> --node <index>
//                   --metadata host:port,host:port,... --data-dir <dir>
//                   [--bootstrap-timeout-ms <n>]
//
// --metadata lists the CLIENT-facing addresses of the metadata group's nodes.
// Everything else (this node's ports, its peers) comes from the shard map.

#include <chrono>
#include <csignal>
#include <cstdint>
#include <cstdlib>
#include <iostream>
#include <string>
#include <thread>
#include <vector>

#include "sharding/shard_node.h"

namespace {

volatile std::sig_atomic_t g_stop_requested = 0;
sharding::ShardNode* g_node = nullptr;

// Async-signal-safe: set a flag, and flip the node's lock-free cancel flag
// so a Start() still waiting for a map returns promptly. The orderly Stop()
// happens on the main thread, NOT here — the Phase 1 server's
// Stop()-then-exit() inside the handler is not async-signal-safe.
void HandleSignal(int) {
    g_stop_requested = 1;
    if (g_node != nullptr) g_node->CancelBootstrap();
}

void PrintUsage() {
    std::cerr << "usage: kv_shard_server --shard <id> --node <index> "
                 "--metadata host:port[,host:port...] --data-dir <dir> "
                 "[--bootstrap-timeout-ms <n>]\n";
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

bool ParseEndpointList(const std::string& text, std::vector<sharding::Endpoint>* out) {
    std::size_t start = 0;
    while (start <= text.size()) {
        std::size_t comma = text.find(',', start);
        if (comma == std::string::npos) comma = text.size();
        sharding::Endpoint endpoint;
        if (!sharding::ParseEndpoint(text.substr(start, comma - start), &endpoint)) return false;
        out->push_back(endpoint);
        start = comma + 1;
    }
    return !out->empty();
}

}  // namespace

int main(int argc, char** argv) {
    sharding::ShardNodeConfig config;
    bool have_shard = false, have_node = false;

    for (int i = 1; i < argc; ++i) {
        const std::string flag = argv[i];
        if (i + 1 >= argc) {
            std::cerr << "missing value for " << flag << "\n";
            PrintUsage();
            return 2;
        }
        const std::string value = argv[++i];
        std::uint64_t number = 0;

        if (flag == "--shard") {
            if (!ParseUnsigned(value, &number) || number > 0xFFFFFFFFull) {
                std::cerr << "bad --shard\n";
                return 2;
            }
            config.shard_id = static_cast<sharding::ShardId>(number);
            have_shard = true;
        } else if (flag == "--node") {
            if (!ParseUnsigned(value, &number)) {
                std::cerr << "bad --node\n";
                return 2;
            }
            config.node_index = static_cast<std::size_t>(number);
            have_node = true;
        } else if (flag == "--metadata") {
            if (!ParseEndpointList(value, &config.metadata_endpoints)) {
                std::cerr << "bad --metadata (expected host:port[,host:port...])\n";
                return 2;
            }
        } else if (flag == "--data-dir") {
            config.data_dir = value;
        } else if (flag == "--bootstrap-timeout-ms") {
            if (!ParseUnsigned(value, &number)) {
                std::cerr << "bad --bootstrap-timeout-ms\n";
                return 2;
            }
            config.bootstrap_timeout = std::chrono::milliseconds(number);
        } else {
            std::cerr << "unknown flag " << flag << "\n";
            PrintUsage();
            return 2;
        }
    }

    if (!have_shard || !have_node || config.metadata_endpoints.empty() || config.data_dir.empty()) {
        PrintUsage();
        return 2;
    }

    sharding::ShardNode node(config);
    g_node = &node;
    std::signal(SIGINT, HandleSignal);
    std::signal(SIGTERM, HandleSignal);

    std::cout << "shard " << config.shard_id << " node " << config.node_index
              << ": waiting for shard map from metadata group...\n";

    const sharding::StartStatus status = node.Start();
    if (status == sharding::StartStatus::kCancelled) {
        std::cout << "cancelled before startup completed\n";
        return 0;
    }
    if (status != sharding::StartStatus::kOk) {
        std::cerr << "startup failed: " << sharding::ToString(status);
        if (!node.detail().empty()) std::cerr << " — " << node.detail();
        std::cerr << "\n";
        return 1;
    }

    std::cout << "serving shard " << config.shard_id << " node " << config.node_index
              << " (map v" << node.map_version() << "). Ctrl+C to stop.\n";

    while (!g_stop_requested) {
        std::this_thread::sleep_for(std::chrono::milliseconds(100));
    }

    std::cout << "stopping...\n";
    node.Stop();
    return 0;
}