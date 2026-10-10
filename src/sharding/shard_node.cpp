#include "sharding/shard_node.h"

#include <filesystem>
#include <map>
#include <thread>

#include "sharding/metadata_client.h"
#include "sharding/shard_map.h"

namespace sharding {
namespace {

using Clock = std::chrono::steady_clock;

static_assert(std::atomic<bool>::is_always_lock_free,
              "CancelBootstrap() must be async-signal-safe");

// Sleeps up to `total`, waking early if cancelled. Returns false if cancelled.
bool SleepUnlessCancelled(std::chrono::milliseconds total, const std::atomic<bool>& cancelled) {
    const auto end = Clock::now() + total;
    while (Clock::now() < end) {
        if (cancelled.load()) return false;
        std::this_thread::sleep_for(std::chrono::milliseconds(20));
    }
    return !cancelled.load();
}

bool ParseNode(const NodeAddr& node, Endpoint* raft_ep, Endpoint* client_ep) {
    return ParseEndpoint(node.raft_addr, raft_ep) && ParseEndpoint(node.client_addr, client_ep);
}

std::string NodeBasePath(const ShardNodeConfig& c) {
    return c.data_dir + "/shard" + std::to_string(c.shard_id) + "_node" +
           std::to_string(c.node_index);
}

}  // namespace

const char* ToString(StartStatus status) {
    switch (status) {
        case StartStatus::kOk: return "ok";
        case StartStatus::kNoMapPublished: return "no shard map available";
        case StartStatus::kCancelled: return "cancelled";
        case StartStatus::kBadMap: return "bad shard map";
        case StartStatus::kUnknownShard: return "shard id not in map";
        case StartStatus::kBadNodeIndex: return "node index not in shard";
        case StartStatus::kBadAddress: return "bad node address in map";
        case StartStatus::kDataDirFailed: return "cannot create data directory";
        case StartStatus::kRaftStartFailed: return "raft node failed to start";
        case StartStatus::kClientServerStartFailed: return "client server failed to start";
    }
    return "unknown";
}

ShardNode::ShardNode(ShardNodeConfig config) : config_(std::move(config)) {}

ShardNode::~ShardNode() { Stop(); }

void ShardNode::Stop() {
    if (client_server_) client_server_->Stop();
    if (raft_node_) raft_node_->Stop();
}

StartStatus ShardNode::Start() {
    // ---- 1. Bootstrap: obtain a shard map ---------------------------------
    MetadataClient metadata(config_.metadata_endpoints, config_.metadata_options);
    const auto deadline = Clock::now() + config_.bootstrap_timeout;

    MetadataClient::FetchResult fetched;
    for (;;) {
        if (cancelled_.load()) return StartStatus::kCancelled;

        fetched = metadata.Fetch();
        if (fetched.status == MetadataClient::FetchStatus::kOk) break;

        if (fetched.status == MetadataClient::FetchStatus::kCorrupt) {
            detail_ = fetched.detail;
            return StartStatus::kBadMap;  // retrying cannot fix a corrupt stored map
        }

        detail_ = (fetched.status == MetadataClient::FetchStatus::kEmpty)
                      ? "no shard map has been published yet"
                      : "metadata group unreachable: " + fetched.detail;

        if (config_.bootstrap_timeout.count() > 0 && Clock::now() >= deadline) {
            return StartStatus::kNoMapPublished;
        }
        if (!SleepUnlessCancelled(config_.bootstrap_retry_interval, cancelled_)) {
            return StartStatus::kCancelled;
        }
    }
    detail_.clear();

    // ---- 2. Validate the map and locate ourselves in it --------------------
    ShardMap map;
    if (ShardMap::Decode(fetched.encoded_map, &map) != DecodeStatus::kOk) {
        detail_ = "stored shard map failed to decode (corrupt or non-canonical)";
        return StartStatus::kBadMap;
    }
    std::string validation_error;
    if (!map.Validate(&validation_error)) {
        detail_ = "stored shard map failed validation: " + validation_error;
        return StartStatus::kBadMap;
    }
    if (map.version != fetched.version) {
        detail_ = "decoded map version disagrees with the version field";
        return StartStatus::kBadMap;
    }

    const ShardInfo* shard = map.Find(config_.shard_id);
    if (shard == nullptr) {
        detail_ = "map v" + std::to_string(map.version) + " has no shard " +
                  std::to_string(config_.shard_id);
        return StartStatus::kUnknownShard;
    }
    if (config_.node_index >= shard->nodes.size()) {
        detail_ = "shard " + std::to_string(config_.shard_id) + " has " +
                  std::to_string(shard->nodes.size()) + " node(s); node index " +
                  std::to_string(config_.node_index) + " is out of range";
        return StartStatus::kBadNodeIndex;
    }

    const std::shared_ptr<const HashRing> ring = map.BuildRing();
    if (!ring || !ring->HasShard(config_.shard_id)) {
        detail_ = "ring built from the map does not contain our shard";
        return StartStatus::kBadMap;
    }

    // ---- 3. Translate the map into a RaftNodeConfig + client peer table ----
    raft::RaftNodeConfig raft_config;
    raft_config.self_id = static_cast<raft::NodeId>(config_.node_index);
    raft_config.election_timeout_min = config_.election_timeout_min;
    raft_config.election_timeout_max = config_.election_timeout_max;
    raft_config.heartbeat_interval = config_.heartbeat_interval;
    // Distinct per (shard, node) so members of a group do not time out in
    // lockstep; never 0 in case 0 means "unseeded".
    raft_config.election_timer_seed =
        1 + static_cast<std::uint64_t>(config_.shard_id) * 64 + config_.node_index;

    std::map<raft::NodeId, raft::PeerAddress> client_peers;
    int own_client_port = 0;
    for (std::size_t i = 0; i < shard->nodes.size(); ++i) {
        Endpoint raft_ep, client_ep;
        if (!ParseNode(shard->nodes[i], &raft_ep, &client_ep)) {
            detail_ = "node " + std::to_string(i) + " of shard " + std::to_string(config_.shard_id) +
                      " has an address that is not host:port";
            return StartStatus::kBadAddress;
        }
        if (i == config_.node_index) {
            raft_config.listen_port = raft_ep.port;
            own_client_port = client_ep.port;
        } else {
            const auto peer_id = static_cast<raft::NodeId>(i);
            raft_config.peers[peer_id] = raft::PeerAddress{raft_ep.host, raft_ep.port};
            client_peers[peer_id] = raft::PeerAddress{client_ep.host, client_ep.port};
        }
    }

    std::error_code ec;
    std::filesystem::create_directories(config_.data_dir, ec);
    if (ec) {
        detail_ = "create_directories(" + config_.data_dir + "): " + ec.message();
        return StartStatus::kDataDirFailed;
    }
    const std::string base = NodeBasePath(config_);
    raft_config.state_path = base + ".state";
    raft_config.log_path = base + ".log";
    raft_config.kv_data_dir = base + ".kv";

    // ---- 4. Start Raft, then the client-facing server ----------------------
    raft_node_ = std::make_unique<raft::RaftNode>(raft_config);
    if (!raft_node_->Start()) {
        detail_ = "RaftNode::Start() failed (is port " + std::to_string(raft_config.listen_port) +
                  " in use?)";
        raft_node_.reset();
        return StartStatus::kRaftStartFailed;
    }

    client_server_ = std::make_unique<raft::ClientServer>(own_client_port, raft_node_.get(),
                                                          std::move(client_peers));

    // Install the guard BEFORE Start(): there must be no moment at which the
    // port is open and unguarded. The lambda owns a reference to the ring, so
    // a later live-refresh step can swap in a new guard without invalidating
    // requests already in flight.
    const ShardId self = config_.shard_id;
    const std::uint64_t version = map.version;
    client_server_->SetKeyGuard([ring, self, version](const std::string& key) -> std::string {
        const std::optional<ShardId> owner = ring->Lookup(key);
        if (owner.has_value() && *owner == self) return "";
        return "WRONG_SHARD " + std::to_string(version) + "\n";
    });

    if (!client_server_->Start()) {
        detail_ = "ClientServer::Start() failed (is port " + std::to_string(own_client_port) +
                  " in use?)";
        client_server_.reset();
        raft_node_->Stop();
        raft_node_.reset();
        return StartStatus::kClientServerStartFailed;
    }

    map_version_ = map.version;
    return StartStatus::kOk;
}

}  // namespace sharding