#pragma once

#include <cstdint>
#include <functional>
#include <memory>
#include <mutex>
#include <string>
#include <unordered_map>
#include <vector>

#include "sharding/hash_ring.h"
#include "sharding/metadata_client.h"
#include "sharding/raft_group_client.h"
#include "sharding/shard_map.h"

namespace sharding {

struct ClientRouterOptions {
    // How the router talks to the metadata group when fetching the map.
    RaftGroupClientOptions metadata_options;
    // Applied to the RaftGroupClient built for every data shard.
    RaftGroupClientOptions shard_options;
    // Upper bound on (route -> WRONG_SHARD -> refetch) rounds per operation.
    // Only rounds in which the cached map actually advanced are retried, so
    // in practice this bounds "the topology kept changing under us".
    int max_route_attempts = 4;
};

// The client-facing entry point of the whole cluster: Put/Get/Delete(key)
// hashes the key on the consistent-hash ring, picks the owning shard's
// RaftGroupClient and delegates. If a shard node rejects the key with
// WRONG_SHARD and a map version NEWER than ours, our map is stale: refetch,
// rebuild, retry.
//
// Thread safety: any number of threads may call Put/Get/Delete/Refresh
// concurrently. The routing state is an immutable Snapshot published through
// a shared_ptr; readers copy the pointer under a short mutex and then work
// without any lock, so a map swap never disturbs an operation in flight.
class ClientRouter {
public:
    using Status = RaftGroupClient::Status;
    using Result = RaftGroupClient::Result;

    enum class RefreshStatus {
        kOk,              // we hold a valid map (possibly the same version as before)
        kNoMapPublished,  // metadata group reachable, nothing published yet
        kUnavailable,     // metadata group unreachable / no commit in time
        kBadMap,          // fetched map is corrupt, invalid, or has bad addresses
    };
    struct RefreshResult {
        RefreshStatus status = RefreshStatus::kUnavailable;
        std::uint64_t version = 0;  // version we hold after this call (0 = none)
        std::string detail;
    };

    ClientRouter(std::vector<Endpoint> metadata_endpoints, ClientRouterOptions options = {});

    // Fetches the map from the metadata group and installs it if it is
    // NEWER than the cached one (an older or equal map is validated and
    // ignored: the cache never goes backwards). Operations call this lazily
    // when no map is cached yet, so calling it up front is optional but
    // gives a clean startup error.
    RefreshResult Refresh();

    Result Put(const std::string& key, const std::string& value);
    Result Get(const std::string& key);
    Result Delete(const std::string& key);

    // 0 if no map has been installed yet.
    std::uint64_t map_version() const;

private:
    // Immutable once published.
    struct Snapshot {
        std::uint64_t version = 0;
        std::shared_ptr<const HashRing> ring;
        std::unordered_map<ShardId, std::vector<Endpoint>> endpoints;
        std::unordered_map<ShardId, std::shared_ptr<RaftGroupClient>> clients;
    };

    std::shared_ptr<const Snapshot> CurrentSnapshot() const;
    std::uint64_t CurrentVersion() const;

    // Refresh, but skip the network entirely if the cached map is already at
    // least `min_version` (0 = always fetch). This is what stops N threads
    // that hit WRONG_SHARD together from doing N refetches.
    RefreshResult RefreshIfBelow(std::uint64_t min_version);

    // Builds a snapshot for `map`, reusing a shard's existing client (and so
    // its cached leader and sequence counter) when its endpoints are
    // unchanged. Returns nullptr and fills `err` on failure.
    static std::shared_ptr<const Snapshot> BuildSnapshot(const ShardMap& map,
                                                         const Snapshot* previous,
                                                         const RaftGroupClientOptions& shard_options,
                                                         std::string* err);

    Result Route(const std::string& key, const std::function<Result(RaftGroupClient&)>& op);

    ClientRouterOptions options_;
    MetadataClient metadata_;

    mutable std::mutex snapshot_mu_;  // guards only the pointer swap/copy
    std::shared_ptr<const Snapshot> snapshot_;

    std::mutex refresh_mu_;  // serializes refreshes (held across the network fetch)
};

}  // namespace sharding