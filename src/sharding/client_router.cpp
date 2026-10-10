#include "sharding/client_router.h"

#include <utility>

namespace sharding {

ClientRouter::ClientRouter(std::vector<Endpoint> metadata_endpoints, ClientRouterOptions options)
    : options_(options), metadata_(std::move(metadata_endpoints), options.metadata_options) {}

std::shared_ptr<const ClientRouter::Snapshot> ClientRouter::CurrentSnapshot() const {
    std::lock_guard<std::mutex> lock(snapshot_mu_);
    return snapshot_;
}

std::uint64_t ClientRouter::CurrentVersion() const {
    const auto snap = CurrentSnapshot();
    return snap ? snap->version : 0;
}

std::uint64_t ClientRouter::map_version() const { return CurrentVersion(); }

std::shared_ptr<const ClientRouter::Snapshot> ClientRouter::BuildSnapshot(
    const ShardMap& map, const Snapshot* previous, const RaftGroupClientOptions& shard_options,
    std::string* err) {
    auto snap = std::make_shared<Snapshot>();
    snap->version = map.version;
    snap->ring = map.BuildRing();
    if (!snap->ring) {
        *err = "ShardMap::BuildRing returned no ring";
        return nullptr;
    }

    for (const ShardInfo& shard : map.shards) {
        std::vector<Endpoint> endpoints;
        for (const NodeAddr& node : shard.nodes) {
            Endpoint ep;
            if (!ParseEndpoint(node.client_addr, &ep)) {
                *err = "shard " + std::to_string(shard.id) + " has a bad client address '" +
                       node.client_addr + "'";
                return nullptr;
            }
            endpoints.push_back(ep);  // order preserved: it is the node identity order
        }
        if (endpoints.empty()) {
            *err = "shard " + std::to_string(shard.id) + " has no nodes";
            return nullptr;
        }

        std::shared_ptr<RaftGroupClient> client;
        if (previous != nullptr) {
            const auto old_eps = previous->endpoints.find(shard.id);
            const auto old_client = previous->clients.find(shard.id);
            if (old_eps != previous->endpoints.end() && old_client != previous->clients.end() &&
                old_eps->second == endpoints) {
                client = old_client->second;  // same group, same addresses: keep leader cache + seq
            }
        }
        if (!client) client = std::make_shared<RaftGroupClient>(endpoints, shard_options);

        snap->endpoints[shard.id] = std::move(endpoints);
        snap->clients[shard.id] = std::move(client);
    }
    return snap;
}

ClientRouter::RefreshResult ClientRouter::Refresh() { return RefreshIfBelow(0); }

ClientRouter::RefreshResult ClientRouter::RefreshIfBelow(std::uint64_t min_version) {
    std::lock_guard<std::mutex> refresh_lock(refresh_mu_);

    // Re-check under the lock: a thread ahead of us in the queue may already
    // have fetched what we were about to fetch.
    const auto current = CurrentSnapshot();
    if (current && min_version != 0 && current->version >= min_version) {
        return {RefreshStatus::kOk, current->version, "already up to date"};
    }
    const std::uint64_t held = current ? current->version : 0;

    const MetadataClient::FetchResult fetched = metadata_.Fetch();
    switch (fetched.status) {
        case MetadataClient::FetchStatus::kOk:
            break;
        case MetadataClient::FetchStatus::kEmpty:
            return {RefreshStatus::kNoMapPublished, held, "no shard map has been published"};
        case MetadataClient::FetchStatus::kCorrupt:
            return {RefreshStatus::kBadMap, held, "metadata group holds a corrupt map: " + fetched.detail};
        case MetadataClient::FetchStatus::kUnavailable:
            return {RefreshStatus::kUnavailable, held, "metadata group unavailable: " + fetched.detail};
    }

    ShardMap map;
    if (ShardMap::Decode(fetched.encoded_map, &map) != DecodeStatus::kOk) {
        return {RefreshStatus::kBadMap, held, "fetched map failed to decode"};
    }
    std::string err;
    if (!map.Validate(&err)) {
        return {RefreshStatus::kBadMap, held, "fetched map failed validation: " + err};
    }
    if (map.version != fetched.version) {
        return {RefreshStatus::kBadMap, held,
                "fetched map version disagrees with its own envelope"};
    }

    // The cache only ever moves forward. (The metadata read is not
    // linearizable — known flag #2 — so an older map can come back.)
    if (current && map.version <= current->version) {
        return {RefreshStatus::kOk, current->version, "fetched map not newer; kept cached"};
    }

    const auto built = BuildSnapshot(map, current.get(), options_.shard_options, &err);
    if (!built) return {RefreshStatus::kBadMap, held, "cannot build routing state: " + err};

    {
        std::lock_guard<std::mutex> lock(snapshot_mu_);
        snapshot_ = built;
    }
    return {RefreshStatus::kOk, built->version, ""};
}

ClientRouter::Result ClientRouter::Route(const std::string& key,
                                         const std::function<Result(RaftGroupClient&)>& op) {
    for (int attempt = 0; attempt < options_.max_route_attempts; ++attempt) {
        auto snap = CurrentSnapshot();
        if (!snap) {
            const RefreshResult r = RefreshIfBelow(1);
            if (r.status != RefreshStatus::kOk) {
                return {Status::kUnavailable, "", "no shard map: " + r.detail, 0};
            }
            snap = CurrentSnapshot();
            if (!snap) return {Status::kUnavailable, "", "no shard map after refresh", 0};
        }

        const auto shard = snap->ring->Lookup(key);
        if (!shard.has_value()) return {Status::kError, "", "shard map has an empty ring", 0};
        const auto it = snap->clients.find(*shard);
        if (it == snap->clients.end()) {
            return {Status::kError, "", "ring chose shard " + std::to_string(*shard) +
                                            " which has no client (router bug)", 0};
        }

        Result result = op(*it->second);
        if (result.status != Status::kWrongShard) return result;

        // Another thread may already have installed a newer map while this
        // operation was in flight on the old one: just route again.
        if (CurrentVersion() > snap->version) continue;

        // The node rejected the key. Decide whether re-fetching can help.
        const std::string who = "shard " + std::to_string(*shard) + " rejected key (node map v" +
                                std::to_string(result.map_version) + ", router map v" +
                                std::to_string(snap->version) + ")";
        if (result.map_version <= snap->version) {
            // The node is not ahead of us, so a refetch would bring nothing
            // new: the node is stale, or router and node rings disagree.
            return {Status::kError, "", who + ": node is not newer than the router's map", 0};
        }

        const RefreshResult r = RefreshIfBelow(result.map_version);
        if (r.status != RefreshStatus::kOk) {
            return {Status::kUnavailable, "", who + "; refetch failed: " + r.detail, 0};
        }
        if (CurrentVersion() <= snap->version) {
            // We asked the metadata group and it has nothing newer than what
            // we already used, yet a node claims to be ahead.
            return {Status::kError, "", who + "; metadata group has no newer map", 0};
        }
        // Map advanced: loop and route again.
    }
    return {Status::kError, "",
            "gave up after " + std::to_string(options_.max_route_attempts) +
                " routing attempts: shard map kept changing",
            0};
}

ClientRouter::Result ClientRouter::Put(const std::string& key, const std::string& value) {
    return Route(key, [&](RaftGroupClient& c) { return c.Put(key, value); });
}

ClientRouter::Result ClientRouter::Get(const std::string& key) {
    return Route(key, [&](RaftGroupClient& c) { return c.Get(key); });
}

ClientRouter::Result ClientRouter::Delete(const std::string& key) {
    return Route(key, [&](RaftGroupClient& c) { return c.Delete(key); });
}

}  // namespace sharding