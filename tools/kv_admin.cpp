// kv_admin — operate on the cluster's shard map in the metadata group.
//
//   kv_admin check   --map-file <path>
//   kv_admin publish --metadata host:port[,host:port...] --map-file <path>
//   kv_admin show    --metadata host:port[,host:port...]
//
// --metadata lists the CLIENT-facing addresses of the metadata group's nodes
// (the same value kv_shard_server takes).
//
// check    parses + validates the map file and prints a summary. Touches no
//          network: use it before publishing.
// publish  encodes the map and stores it, but only if its version is strictly
//          greater than the one already stored. Reads it back to confirm.
// show     fetches the stored map and prints it in map-file format (so the
//          output can be edited and published again).
//
// Exit codes: 0 ok | 1 bad input or corrupt data | 2 usage | 3 conflict
//             (stale version, or nothing published) | 4 metadata group
//             unavailable | 5 stored map differs from what was published.

#include <cstdint>
#include <fstream>
#include <iostream>
#include <sstream>
#include <string>
#include <vector>

#include "sharding/metadata_client.h"
#include "sharding/raft_group_client.h"
#include "sharding/shard_map.h"
#include "sharding/topology_text.h"

namespace {

using sharding::Endpoint;
using sharding::MetadataClient;
using sharding::ShardMap;

void PrintUsage() {
    std::cerr << "usage:\n"
                 "  kv_admin check   --map-file <path>\n"
                 "  kv_admin publish --metadata host:port[,host:port...] --map-file <path>\n"
                 "  kv_admin show    --metadata host:port[,host:port...]\n";
}

bool ParseEndpointList(const std::string& text, std::vector<Endpoint>* out) {
    std::size_t start = 0;
    while (start <= text.size()) {
        std::size_t comma = text.find(',', start);
        if (comma == std::string::npos) comma = text.size();
        Endpoint endpoint;
        if (!sharding::ParseEndpoint(text.substr(start, comma - start), &endpoint)) return false;
        out->push_back(endpoint);
        start = comma + 1;
    }
    return !out->empty();
}

bool ReadFile(const std::string& path, std::string* out) {
    std::ifstream in(path);
    if (!in) return false;
    std::ostringstream buffer;
    buffer << in.rdbuf();
    *out = buffer.str();
    return true;
}

std::size_t CountNodes(const ShardMap& map) {
    std::size_t n = 0;
    for (const auto& shard : map.shards) n += shard.nodes.size();
    return n;
}

// Map file -> parsed + validated ShardMap + its encoding. 0 on success.
int LoadMapFile(const std::string& path, ShardMap* map, std::string* encoded) {
    std::string text;
    if (!ReadFile(path, &text)) {
        std::cerr << "cannot read map file '" << path << "'\n";
        return 1;
    }
    std::string err;
    if (!sharding::ParseMapFile(text, map, &err)) {
        std::cerr << path << ": " << err << "\n";
        return 1;
    }
    if (!map->Validate(&err)) {
        std::cerr << path << ": map is not valid: " << err << "\n";
        return 1;
    }
    *encoded = map->Encode();
    return 0;
}

int RunCheck(const std::string& map_file) {
    ShardMap map;
    std::string encoded;
    if (const int rc = LoadMapFile(map_file, &map, &encoded)) return rc;
    std::cout << "ok: map v" << map.version << ", " << map.shards.size() << " shard(s), "
              << CountNodes(map) << " node(s), " << map.vnodes << " vnodes/shard\n";
    return 0;
}

int RunPublish(const std::vector<Endpoint>& metadata, const std::string& map_file) {
    ShardMap map;
    std::string encoded;
    if (const int rc = LoadMapFile(map_file, &map, &encoded)) return rc;

    MetadataClient client(metadata);
    const MetadataClient::PublishResult result = client.Publish(encoded);
    switch (result.status) {
        case MetadataClient::PublishStatus::kOk:
            break;
        case MetadataClient::PublishStatus::kStaleVersion:
            std::cerr << "refused: the metadata group already holds map v" << result.current_version
                      << "; this file is v" << map.version
                      << ". Raise 'version' above " << result.current_version << " and retry.\n";
            return 3;
        case MetadataClient::PublishStatus::kMalformed:
            std::cerr << "refused: the map being published has no parsable version\n";
            return 1;
        case MetadataClient::PublishStatus::kCorruptStored:
            std::cerr << "refused: the map currently stored is corrupt (no parsable version): "
                      << result.detail << "\n";
            return 1;
        case MetadataClient::PublishStatus::kUnavailable:
            std::cerr << "metadata group unavailable: " << result.detail << "\n";
            return 4;
    }

    // Publish is read-then-write, not compare-and-swap (known flag #3): read
    // back to confirm that what is stored is what we wrote.
    const MetadataClient::FetchResult back = client.Fetch();
    if (back.status != MetadataClient::FetchStatus::kOk || back.encoded_map != encoded) {
        std::cerr << "published v" << map.version
                  << " but the read-back did not match (another publisher racing, or a stale read). "
                     "Run 'kv_admin show' to see what is stored.\n";
        return 5;
    }
    std::cout << "published map v" << map.version << " (" << map.shards.size() << " shard(s), "
              << CountNodes(map) << " node(s)); verified by read-back\n";
    return 0;
}

int RunShow(const std::vector<Endpoint>& metadata) {
    MetadataClient client(metadata);
    const MetadataClient::FetchResult fetched = client.Fetch();
    switch (fetched.status) {
        case MetadataClient::FetchStatus::kOk:
            break;
        case MetadataClient::FetchStatus::kEmpty:
            std::cerr << "nothing has been published yet\n";
            return 3;
        case MetadataClient::FetchStatus::kCorrupt:
            std::cerr << "the stored map is corrupt: " << fetched.detail << "\n";
            return 1;
        case MetadataClient::FetchStatus::kUnavailable:
            std::cerr << "metadata group unavailable: " << fetched.detail << "\n";
            return 4;
    }
    ShardMap map;
    if (ShardMap::Decode(fetched.encoded_map, &map) != sharding::DecodeStatus::kOk) {
        std::cerr << "the stored map failed to decode (corrupt or non-canonical)\n";
        return 1;
    }
    std::cout << sharding::FormatMapFile(map);
    return 0;
}

}  // namespace

int main(int argc, char** argv) {
    if (argc < 2) {
        PrintUsage();
        return 2;
    }
    const std::string command = argv[1];
    if (command != "check" && command != "publish" && command != "show") {
        std::cerr << "unknown command " << command << "\n";
        PrintUsage();
        return 2;
    }

    std::vector<Endpoint> metadata;
    std::string map_file;
    for (int i = 2; i < argc; ++i) {
        const std::string flag = argv[i];
        if (i + 1 >= argc) {
            std::cerr << "missing value for " << flag << "\n";
            PrintUsage();
            return 2;
        }
        const std::string value = argv[++i];
        if (flag == "--metadata") {
            if (!ParseEndpointList(value, &metadata)) {
                std::cerr << "bad --metadata (expected host:port[,host:port...])\n";
                return 2;
            }
        } else if (flag == "--map-file") {
            map_file = value;
        } else {
            std::cerr << "unknown flag " << flag << "\n";
            PrintUsage();
            return 2;
        }
    }

    if (command == "check") {
        if (map_file.empty()) {
            PrintUsage();
            return 2;
        }
        return RunCheck(map_file);
    }
    if (metadata.empty()) {
        PrintUsage();
        return 2;
    }
    if (command == "publish") {
        if (map_file.empty()) {
            PrintUsage();
            return 2;
        }
        return RunPublish(metadata, map_file);
    }
    return RunShow(metadata);
}