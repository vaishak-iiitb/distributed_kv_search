// ShardNode against a scripted metadata store: bootstrap behaviour, address
// derivation, and key-ownership enforcement over the real wire.
//
// The metadata group is FAKED here on purpose (a one-key store speaking the
// real line protocol). Real metadata Raft + real shard Raft together is the
// 12-process integration test; this one isolates ShardNode's own logic.

#include <atomic>
#include <chrono>
#include <cstdio>
#include <filesystem>
#include <functional>
#include <memory>
#include <mutex>
#include <optional>
#include <string>
#include <thread>
#include <vector>

#include "../unit/sharding/fake_line_server.h"
#include "net/line_client.h"
#include "sharding/metadata_client.h"
#include "sharding/raft_group_client.h"
#include "sharding/shard_map.h"
#include "sharding/shard_node.h"

namespace {

int g_failures = 0;

#define CHECK(cond)                                                                    \
    do {                                                                               \
        if (!(cond)) {                                                                 \
            std::fprintf(stderr, "%s:%d: CHECK failed: %s\n", __FILE__, __LINE__, #cond); \
            ++g_failures;                                                              \
        }                                                                              \
    } while (0)

using Clock = std::chrono::steady_clock;
using sharding::Endpoint;
using sharding::RaftGroupClient;
using sharding::RaftGroupClientOptions;
using sharding::ShardNode;
using sharding::ShardNodeConfig;
using sharding::StartStatus;

bool WaitUntil(const std::function<bool()>& predicate, std::chrono::milliseconds timeout) {
    const auto deadline = Clock::now() + timeout;
    while (Clock::now() < deadline) {
        if (predicate()) return true;
        std::this_thread::sleep_for(std::chrono::milliseconds(20));
    }
    return predicate();
}

// One-key stand-in for the metadata group's leader (real line protocol).
struct FakeMetadataStore {
    std::mutex mu;
    std::optional<std::string> map;

    void Set(const std::string& encoded) {
        std::lock_guard<std::mutex> lock(mu);
        map = encoded;
    }

    std::optional<std::string> Handle(const std::string& line) {
        std::lock_guard<std::mutex> lock(mu);
        if (line == "LEADER") return "LEADER";
        if (line == std::string("GET ") + sharding::kShardMapKey) {
            return map.has_value() ? "VALUE " + *map : "NOT_FOUND";
        }
        return "ERROR unsupported in fake";
    }
};

// Two shards (0 and 1), three nodes each. Only shard 0's nodes are ever
// started; shard 1 exists so that some keys are NOT owned by shard 0.
sharding::ShardMap MakeMap(std::uint64_t version, int port_base) {
    sharding::ShardMap map;
    map.version = version;
    map.vnodes = 64;
    for (sharding::ShardId shard = 0; shard < 2; ++shard) {
        sharding::ShardInfo info;
        info.id = shard;
        for (int node = 0; node < 3; ++node) {
            const int offset = static_cast<int>(shard) * 20 + node;
            info.nodes.push_back(sharding::NodeAddr{
                "127.0.0.1:" + std::to_string(port_base + offset),         // raft
                "127.0.0.1:" + std::to_string(port_base + 10 + offset)});  // client
        }
        map.shards.push_back(info);
    }
    return map;
}

int ClientPort(int port_base, sharding::ShardId shard, int node) {
    return port_base + 10 + static_cast<int>(shard) * 20 + node;
}

ShardNodeConfig BaseConfig(const FakeLineServer& metadata, const std::string& data_dir,
                           sharding::ShardId shard, std::size_t node) {
    ShardNodeConfig c;
    c.shard_id = shard;
    c.node_index = node;
    c.metadata_endpoints = {Endpoint{"127.0.0.1", metadata.port()}};
    c.data_dir = data_dir;
    c.bootstrap_retry_interval = std::chrono::milliseconds(50);
    c.election_timeout_min = raft::Duration(100);
    c.election_timeout_max = raft::Duration(200);
    c.heartbeat_interval = raft::Duration(30);
    return c;
}

void Clean(const std::string& dir) {
    std::error_code ec;
    std::filesystem::remove_all(dir, ec);
}

// ---------------------------------------------------------------------------

void test_nodes_start_and_enforce_ownership() {
    const int kBase = 19600;
    const std::string dir = "shard_node_test_data_a";
    Clean(dir);

    const sharding::ShardMap map = MakeMap(3, kBase);
    std::string err;
    CHECK(map.Validate(&err));
    FakeMetadataStore store;
    store.Set(map.Encode());
    FakeLineServer metadata([&store](const std::string& l) { return store.Handle(l); });

    std::vector<std::unique_ptr<ShardNode>> nodes;
    for (std::size_t i = 0; i < 3; ++i) {
        nodes.push_back(std::make_unique<ShardNode>(BaseConfig(metadata, dir, 0, i)));
        const StartStatus status = nodes.back()->Start();
        if (status != StartStatus::kOk) {
            std::fprintf(stderr, "node %zu start: %s (%s)\n", i, sharding::ToString(status),
                         nodes.back()->detail().c_str());
        }
        CHECK(status == StartStatus::kOk);
        CHECK(nodes.back()->map_version() == 3);
    }

    // Pick one key each shard owns, from the same ring the nodes use.
    const auto ring = map.BuildRing();
    std::string mine, theirs;
    for (int i = 0; i < 1000 && (mine.empty() || theirs.empty()); ++i) {
        const std::string key = "key-" + std::to_string(i);
        const auto owner = ring->Lookup(key);
        if (owner == 0u && mine.empty()) mine = key;
        if (owner == 1u && theirs.empty()) theirs = key;
    }
    CHECK(!mine.empty() && !theirs.empty());
    if (mine.empty() || theirs.empty()) return;

    // Wait for the 3-node shard to elect a leader.
    std::optional<std::size_t> leader;
    CHECK(WaitUntil(
        [&] {
            for (std::size_t i = 0; i < 3; ++i) {
                if (nodes[i]->raft_node()->CurrentRole() == raft::Role::kLeader) {
                    leader = i;
                    return true;
                }
            }
            return false;
        },
        std::chrono::milliseconds(4000)));
    if (!leader) return;

    std::vector<Endpoint> endpoints;
    for (int i = 0; i < 3; ++i) endpoints.push_back({"127.0.0.1", ClientPort(kBase, 0, i)});
    RaftGroupClientOptions options;
    options.overall_timeout_ms = 8000;
    RaftGroupClient client(endpoints, options);

    // Owned key: normal behaviour, through the real shard Raft group.
    CHECK(client.Put(mine, "v1").status == RaftGroupClient::Status::kOk);
    const auto got = client.Get(mine);
    CHECK(got.status == RaftGroupClient::Status::kOk && got.value == "v1");

    // Foreign key: every verb is refused as kWrongShard, carrying the node's map
    // version both parsed (map_version) and raw (detail).
    const auto put_foreign = client.Put(theirs, "x");
    CHECK(put_foreign.status == RaftGroupClient::Status::kWrongShard);
    CHECK(put_foreign.map_version == 3);
    CHECK(put_foreign.detail == "WRONG_SHARD 3");
    const auto del_foreign = client.Delete(theirs);
    CHECK(del_foreign.status == RaftGroupClient::Status::kWrongShard);
    CHECK(del_foreign.map_version == 3);
    CHECK(del_foreign.detail == "WRONG_SHARD 3");
    const auto get_foreign = client.Get(theirs);
    CHECK(get_foreign.status == RaftGroupClient::Status::kWrongShard);
    CHECK(get_foreign.map_version == 3);
    CHECK(get_foreign.detail == "WRONG_SHARD 3");

    // A rejection consumed a sequence number client-side but nothing server-side;
    // the next real write must still go through (a gap in seq is fine).
    CHECK(client.Put(mine, "v2").status == RaftGroupClient::Status::kOk);
    const auto got2 = client.Get(mine);
    CHECK(got2.status == RaftGroupClient::Status::kOk && got2.value == "v2");

    // The LEADER probe is keyless and unaffected by the guard.
    const auto probe = net::LineClient::RoundTrip("127.0.0.1", ClientPort(kBase, 0, static_cast<int>(*leader)),
                                                  "LEADER", 1000);
    CHECK(probe.has_value() && *probe == "LEADER");

    // The foreign key never entered any node's local store, while the owned
    // key replicated everywhere.
    for (std::size_t i = 0; i < 3; ++i) {
        std::string value;
        CHECK(WaitUntil([&] { return nodes[i]->raft_node()->Get(mine, &value) && value == "v2"; },
                        std::chrono::milliseconds(3000)));
        CHECK(!nodes[i]->raft_node()->Get(theirs, &value));
    }

    for (auto& n : nodes) n->Stop();
    Clean(dir);
}

void test_start_waits_for_a_late_published_map() {
    const int kBase = 19700;
    const std::string dir = "shard_node_test_data_b";
    Clean(dir);

    FakeMetadataStore store;  // nothing published yet
    FakeLineServer metadata([&store](const std::string& l) { return store.Handle(l); });

    ShardNodeConfig config = BaseConfig(metadata, dir, 0, 0);
    config.bootstrap_timeout = std::chrono::milliseconds(8000);
    ShardNode node(config);

    StartStatus status = StartStatus::kBadMap;
    const auto start = Clock::now();
    std::thread starter([&] { status = node.Start(); });

    std::this_thread::sleep_for(std::chrono::milliseconds(400));
    store.Set(MakeMap(7, kBase).Encode());
    starter.join();

    const long elapsed_ms =
        std::chrono::duration_cast<std::chrono::milliseconds>(Clock::now() - start).count();
    CHECK(status == StartStatus::kOk);
    CHECK(elapsed_ms >= 350);  // it genuinely waited; it did not serve map-less
    CHECK(node.map_version() == 7);

    node.Stop();
    Clean(dir);
}

void test_start_times_out_without_a_map_and_serves_nothing() {
    const int kBase = 19800;
    const std::string dir = "shard_node_test_data_c";
    Clean(dir);

    FakeMetadataStore store;
    FakeLineServer metadata([&store](const std::string& l) { return store.Handle(l); });

    ShardNodeConfig config = BaseConfig(metadata, dir, 0, 0);
    config.bootstrap_timeout = std::chrono::milliseconds(300);
    ShardNode node(config);

    CHECK(node.Start() == StartStatus::kNoMapPublished);
    CHECK(!node.detail().empty());
    CHECK(node.raft_node() == nullptr);
    // Its (would-be) client port must not be open.
    CHECK(!net::LineClient::RoundTrip("127.0.0.1", ClientPort(kBase, 0, 0), "LEADER", 300).has_value());
    Clean(dir);
}

void test_unreachable_metadata_group_times_out() {
    const std::string dir = "shard_node_test_data_d";
    Clean(dir);

    ShardNodeConfig config;
    config.shard_id = 0;
    config.node_index = 0;
    config.metadata_endpoints = {Endpoint{"127.0.0.1", UnusedPort()}};
    config.data_dir = dir;
    config.bootstrap_timeout = std::chrono::milliseconds(300);
    config.bootstrap_retry_interval = std::chrono::milliseconds(50);
    config.metadata_options.overall_timeout_ms = 200;  // keep the test quick
    ShardNode node(config);

    CHECK(node.Start() == StartStatus::kNoMapPublished);
    CHECK(node.detail().find("unreachable") != std::string::npos);
    Clean(dir);
}

void test_bad_inputs_are_rejected_before_anything_starts() {
    const int kBase = 19900;
    const std::string dir = "shard_node_test_data_e";
    Clean(dir);

    FakeMetadataStore store;
    store.Set(MakeMap(1, kBase).Encode());
    FakeLineServer metadata([&store](const std::string& l) { return store.Handle(l); });

    {  // shard id absent from the map
        ShardNode node(BaseConfig(metadata, dir, 7, 0));
        CHECK(node.Start() == StartStatus::kUnknownShard);
        CHECK(node.raft_node() == nullptr);
    }
    {  // node index beyond the shard's node list
        ShardNode node(BaseConfig(metadata, dir, 0, 5));
        CHECK(node.Start() == StartStatus::kBadNodeIndex);
        CHECK(node.raft_node() == nullptr);
    }
    {  // version parses (so Fetch succeeds) but the map does not decode
        store.Set("SM1;v=1;this-is-not-a-real-map");
        ShardNode node(BaseConfig(metadata, dir, 0, 0));
        CHECK(node.Start() == StartStatus::kBadMap);
    }
    {  // no parsable version at all: Fetch itself reports corruption
        store.Set("garbage");
        ShardNode node(BaseConfig(metadata, dir, 0, 0));
        CHECK(node.Start() == StartStatus::kBadMap);
    }
    Clean(dir);
}

void test_cancel_interrupts_a_waiting_start() {
    const std::string dir = "shard_node_test_data_f";
    Clean(dir);

    FakeMetadataStore store;  // never published
    FakeLineServer metadata([&store](const std::string& l) { return store.Handle(l); });

    ShardNode node(BaseConfig(metadata, dir, 0, 0));  // bootstrap_timeout 0 = forever
    StartStatus status = StartStatus::kOk;
    std::thread starter([&] { status = node.Start(); });

    std::this_thread::sleep_for(std::chrono::milliseconds(200));
    const auto cancelled_at = Clock::now();
    node.CancelBootstrap();
    starter.join();

    CHECK(status == StartStatus::kCancelled);
    CHECK(std::chrono::duration_cast<std::chrono::milliseconds>(Clock::now() - cancelled_at).count() < 1500);
    Clean(dir);
}

}  // namespace

int main() {
    test_nodes_start_and_enforce_ownership();
    test_start_waits_for_a_late_published_map();
    test_start_times_out_without_a_map_and_serves_nothing();
    test_unreachable_metadata_group_times_out();
    test_bad_inputs_are_rejected_before_anything_starts();
    test_cancel_interrupts_a_waiting_start();

    if (g_failures == 0) {
        std::printf("shard_node_test: all checks passed\n");
        return 0;
    }
    std::fprintf(stderr, "shard_node_test: %d check(s) failed\n", g_failures);
    return 1;
}