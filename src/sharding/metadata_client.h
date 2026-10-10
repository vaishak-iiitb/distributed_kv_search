#pragma once

#include <cstdint>
#include <string>
#include <vector>

#include "sharding/raft_group_client.h"

namespace sharding {

// The one key the metadata Raft group stores. The group is an ordinary Raft
// group running the ordinary ClientServer: it needs no special server code,
// because it only ever holds this single key mapping to an encoded ShardMap.
inline constexpr const char* kShardMapKey = "shardmap";

// Extracts the `v=` field from an encoded ShardMap ("SM1;v=7;vn=..;...").
// Only looks at the version — full validation (checksum, canonical form) is
// ShardMap::Decode's job and happens where the map is actually consumed.
// Strict: requires the "SM1;" prefix, a `v=` token (NOT `vn=`), digits only,
// no overflow.
bool ParseShardMapVersion(const std::string& encoded, std::uint64_t* version);

// Publishes and fetches the cluster's shard map through the metadata group.
// Treats the map as an opaque encoded string plus its version number.
class MetadataClient {
public:
    explicit MetadataClient(std::vector<Endpoint> metadata_endpoints,
                            RaftGroupClientOptions options = {});

    enum class PublishStatus {
        kOk,
        kStaleVersion,   // new version <= the version already published
        kMalformed,      // the map being published has no parsable version
        kCorruptStored,  // the map currently stored has no parsable version
        kUnavailable,    // metadata group unreachable / no commit in time
    };
    struct PublishResult {
        PublishStatus status = PublishStatus::kUnavailable;
        std::uint64_t current_version = 0;  // valid for kStaleVersion
        std::string detail;
    };

    // Stores `encoded_map` iff its version is STRICTLY greater than the
    // stored one (or nothing is stored yet). This is a read-then-write, NOT
    // an atomic compare-and-swap: two publishers racing can both pass the
    // check (known flag #3 — documented, not fixed).
    PublishResult Publish(const std::string& encoded_map);

    enum class FetchStatus {
        kOk,
        kEmpty,        // nothing has ever been published
        kCorrupt,      // stored value has no parsable version
        kUnavailable,
    };
    struct FetchResult {
        FetchStatus status = FetchStatus::kUnavailable;
        std::string encoded_map;
        std::uint64_t version = 0;
        std::string detail;
    };
    FetchResult Fetch();

private:
    RaftGroupClient group_;
};

}  // namespace sharding