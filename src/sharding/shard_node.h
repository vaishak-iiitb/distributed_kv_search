#pragma once

#include <atomic>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <string>
#include <vector>

#include "raft/client_server.h"
#include "raft/raft_node.h"
#include "sharding/hash_ring.h"
#include "sharding/raft_group_client.h"

namespace sharding {

struct ShardNodeConfig {
    // Which shard this process serves, and which member of that shard it is.
    // node_index is the position in the shard's node list in the published
    // ShardMap, and it BECOMES the Raft NodeId. See the "node order is
    // identity" note in the Step 4 guide before ever reordering a node list.
    ShardId shard_id = 0;
    std::size_t node_index = 0;

    // Client-facing addresses of the metadata group's nodes.
    std::vector<Endpoint> metadata_endpoints;

    // Directory for this node's Raft state, Raft log and KV engine.
    std::string data_dir;

    // How long Start() keeps waiting for a shard map to be published and
    // reachable. 0 = wait forever (the production default: a shard node
    // started before the metadata group is up should simply wait).
    std::chrono::milliseconds bootstrap_timeout{0};
    std::chrono::milliseconds bootstrap_retry_interval{200};

    // Per-Fetch budget during bootstrap, so one dead metadata node cannot
    // stall the retry loop (or a cancel request) for long.
    RaftGroupClientOptions metadata_options = MakeBootstrapOptions();

    raft::Duration election_timeout_min = raft::Duration(150);
    raft::Duration election_timeout_max = raft::Duration(300);
    raft::Duration heartbeat_interval = raft::Duration(50);

    static RaftGroupClientOptions MakeBootstrapOptions() {
        RaftGroupClientOptions o;
        o.attempt_timeout_ms = 1000;
        o.overall_timeout_ms = 3000;
        o.retry_backoff_ms = 50;
        return o;
    }
};

enum class StartStatus {
    kOk,
    kNoMapPublished,   // bootstrap_timeout elapsed: unreachable or nothing published
    kCancelled,        // CancelBootstrap() was called
    kBadMap,           // fetched map fails Decode / Validate / consistency checks
    kUnknownShard,     // the map has no shard with our shard_id
    kBadNodeIndex,     // our shard has no node at node_index
    kBadAddress,       // a node address in the map is not "host:port"
    kDataDirFailed,
    kRaftStartFailed,
    kClientServerStartFailed,
};

const char* ToString(StartStatus status);

// One member of one data shard: fetches the shard map, derives its own Raft
// and client addresses and its peers from it, starts a RaftNode + ClientServer,
// and installs a key guard that rejects keys the shard does not own.
class ShardNode {
public:
    explicit ShardNode(ShardNodeConfig config);
    ~ShardNode();

    ShardNode(const ShardNode&) = delete;
    ShardNode& operator=(const ShardNode&) = delete;

    // Blocks until the node is serving, or fails. Call at most once.
    StartStatus Start();

    // Stops serving. Idempotent.
    void Stop();

    // Makes a blocked Start() return kCancelled. A plain atomic store, so
    // it is safe to call from a signal handler.
    void CancelBootstrap() { cancelled_.store(true); }

    // Human-readable reason for the last failure or retry (empty if none).
    const std::string& detail() const { return detail_; }

    std::uint64_t map_version() const { return map_version_; }
    raft::RaftNode* raft_node() { return raft_node_.get(); }

private:
    ShardNodeConfig config_;
    std::atomic<bool> cancelled_{false};
    std::string detail_;
    std::uint64_t map_version_ = 0;

    // Declared raft_node_ first, client_server_ second: members are
    // destroyed in reverse, so the ClientServer (which holds a raw pointer
    // to the RaftNode) always dies before the node it talks to.
    std::unique_ptr<raft::RaftNode> raft_node_;
    std::unique_ptr<raft::ClientServer> client_server_;
};

}  // namespace sharding