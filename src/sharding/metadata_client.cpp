#include "sharding/metadata_client.h"

#include <limits>

namespace sharding {

bool ParseShardMapVersion(const std::string& encoded, std::uint64_t* version) {
    const std::string kPrefix = "SM1;";
    if (encoded.compare(0, kPrefix.size(), kPrefix) != 0) return false;

    // Walk the ';'-separated tokens after the prefix. Matching whole tokens
    // that START with "v=" is what keeps "vn=64" from being mistaken for it.
    std::size_t pos = kPrefix.size();
    while (pos <= encoded.size()) {
        std::size_t end = encoded.find(';', pos);
        if (end == std::string::npos) end = encoded.size();
        const std::string token = encoded.substr(pos, end - pos);

        if (token.compare(0, 2, "v=") == 0) {
            const std::string digits = token.substr(2);
            if (digits.empty()) return false;
            std::uint64_t value = 0;
            for (char c : digits) {
                if (c < '0' || c > '9') return false;
                const std::uint64_t d = static_cast<std::uint64_t>(c - '0');
                if (value > (std::numeric_limits<std::uint64_t>::max() - d) / 10) return false;
                value = value * 10 + d;
            }
            *version = value;
            return true;
        }
        if (end == encoded.size()) break;
        pos = end + 1;
    }
    return false;
}

MetadataClient::MetadataClient(std::vector<Endpoint> metadata_endpoints,
                               RaftGroupClientOptions options)
    : group_(std::move(metadata_endpoints), options) {}

MetadataClient::FetchResult MetadataClient::Fetch() {
    FetchResult out;
    const auto got = group_.Get(kShardMapKey);

    if (got.status == RaftGroupClient::Status::kNotFound) {
        out.status = FetchStatus::kEmpty;
        return out;
    }
    if (got.status != RaftGroupClient::Status::kOk) {
        out.status = FetchStatus::kUnavailable;
        out.detail = got.detail;
        return out;
    }

    std::uint64_t version = 0;
    if (!ParseShardMapVersion(got.value, &version)) {
        out.status = FetchStatus::kCorrupt;
        out.detail = "stored shard map has no parsable version";
        return out;
    }
    out.status = FetchStatus::kOk;
    out.encoded_map = got.value;
    out.version = version;
    return out;
}

MetadataClient::PublishResult MetadataClient::Publish(const std::string& encoded_map) {
    PublishResult out;

    std::uint64_t new_version = 0;
    if (!ParseShardMapVersion(encoded_map, &new_version)) {
        out.status = PublishStatus::kMalformed;
        out.detail = "map to publish has no parsable version";
        return out;
    }

    // Read-modify-write: this read and the write below are two separate
    // Raft operations with nothing holding the key in between (flag #3).
    const FetchResult current = Fetch();
    switch (current.status) {
        case FetchStatus::kUnavailable:
            out.status = PublishStatus::kUnavailable;
            out.detail = current.detail;
            return out;
        case FetchStatus::kCorrupt:
            out.status = PublishStatus::kCorruptStored;
            out.detail = current.detail;
            return out;
        case FetchStatus::kOk:
            if (new_version <= current.version) {
                out.status = PublishStatus::kStaleVersion;
                out.current_version = current.version;
                return out;
            }
            break;
        case FetchStatus::kEmpty:
            break;
    }

    const auto put = group_.Put(kShardMapKey, encoded_map);
    if (put.status == RaftGroupClient::Status::kOk) {
        out.status = PublishStatus::kOk;
        return out;
    }
    out.status = PublishStatus::kUnavailable;
    out.detail = put.detail;
    return out;
}

}  // namespace sharding