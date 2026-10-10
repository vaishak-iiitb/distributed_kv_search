// Tests for RaftGroupClient's kWrongShard status and for ClientRouter.
// No Raft anywhere: every server is a scripted FakeLineServer, so each
// scenario (stale router, stale node, endless map churn, ...) is exact and
// repeatable. Plain main() + CHECK, same spirit as the other sharding tests.

#include <atomic>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <functional>
#include <iostream>
#include <memory>
#include <mutex>
#include <optional>
#include <sstream>
#include <string>
#include <thread>
#include <utility>
#include <vector>

#include "fake_line_server.h"
#include "sharding/client_router.h"
#include "sharding/raft_group_client.h"
#include "sharding/shard_map.h"

using namespace sharding;

static int g_failures = 0;
#define CHECK(cond)                                                                   \
    do {                                                                              \
        if (!(cond)) {                                                                \
            std::cerr << "  CHECK FAILED " << __FILE__ << ":" << __LINE__ << "  " #cond \
                      << std::endl;                                                   \
            ++g_failures;                                                             \
        }                                                                             \
    } while (0)

namespace {

// ---------- helpers ----------

RaftGroupClientOptions FastOptions() {
    RaftGroupClientOptions o;
    o.attempt_timeout_ms = 500;
    o.overall_timeout_ms = 2000;
    o.retry_backoff_ms = 10;
    return o;
}

ClientRouterOptions FastRouterOptions(int max_attempts = 4) {
    ClientRouterOptions o;
    o.metadata_options = FastOptions();
    o.shard_options = FastOptions();
    o.max_route_attempts = max_attempts;
    return o;
}

Endpoint Ep(int port) { return Endpoint{"127.0.0.1", port}; }

// shards: (shard id, client ports of its nodes). Raft addresses are made up
// but distinct; the router never dials them.
ShardMap MakeMap(std::uint64_t version, const std::vector<std::pair<ShardId, std::vector<int>>>& shards) {
    ShardMap m;
    m.version = version;
    m.vnodes = 64;
    for (const auto& [id, ports] : shards) {
        ShardInfo info;
        info.id = id;
        for (std::size_t i = 0; i < ports.size(); ++i) {
            info.nodes.push_back(
                {"127.0.0.1:" + std::to_string(30000 + id * 10 + i), "127.0.0.1:" + std::to_string(ports[i])});
        }
        m.shards.push_back(info);
    }
    return m;
}

std::string EncodeChecked(const ShardMap& m) {
    std::string err;
    if (!m.Validate(&err)) {
        std::cerr << "test bug: map invalid: " << err << std::endl;
        std::exit(2);
    }
    return m.Encode();
}

// Distinct keys that the ring built from `m` assigns to `shard`.
std::vector<std::string> KeysOwnedBy(const ShardMap& m, ShardId shard, std::size_t n) {
    const auto ring = m.BuildRing();
    std::vector<std::string> keys;
    for (int i = 0; keys.size() < n && i < 100000; ++i) {
        const std::string key = "key-" + std::to_string(i);
        if (ring->Lookup(key) == std::optional<ShardId>(shard)) keys.push_back(key);
    }
    return keys;
}

// Scripted metadata "group": LEADER probe + GET shardmap, like a real one.
// `supplier`, if set, produces the encoded map on every GET (to script churn).
struct FakeMetadata {
    explicit FakeMetadata(std::string initial) : encoded(std::move(initial)) {}

    void Set(std::string e) {
        std::lock_guard<std::mutex> l(mu);
        encoded = std::move(e);
        supplier = nullptr;
    }

    std::mutex mu;
    std::string encoded;
    std::function<std::string(int fetch_number)> supplier;
    std::string raw_reply_override;  // if non-empty, sent verbatim instead of VALUE <map>
    std::atomic<int> fetches{0};

    FakeLineServer server{[this](const std::string& line) -> std::optional<std::string> {
        if (line == "LEADER") return std::string("LEADER");
        if (line == std::string("GET ") + kShardMapKey) {
            const int n = ++fetches;
            std::lock_guard<std::mutex> l(mu);
            if (!raw_reply_override.empty()) return raw_reply_override;
            if (supplier) return "VALUE " + supplier(n);
            return "VALUE " + encoded;
        }
        return std::string("ERROR unexpected");
    }};

    std::vector<Endpoint> endpoints() const { return {Ep(server.port())}; }
};

// A scripted data-shard node: answers the LEADER probe itself and hands every
// other line to `on_request`.
struct FakeShard {
    explicit FakeShard(std::function<std::string(const std::string&)> on_request)
        : on_request_(std::move(on_request)) {}

    std::function<std::string(const std::string&)> on_request_;
    FakeLineServer server{[this](const std::string& line) -> std::optional<std::string> {
        if (line == "LEADER") return std::string("LEADER");
        return on_request_(line);
    }};

    int port() const { return server.port(); }
    std::vector<std::string> received() const { return server.received(); }
};

std::string OkEverything(const std::string& line) {
    if (line.rfind("GET ", 0) == 0) return "VALUE v-" + line.substr(4);
    return "OK";
}

// Lines of the form "<VERB> <cid> <seq> <key> ..." -> tokens.
std::vector<std::string> Tokens(const std::string& line) {
    std::istringstream in(line);
    std::vector<std::string> t;
    std::string w;
    while (in >> w) t.push_back(w);
    return t;
}

int CountLinesWithKey(const std::vector<std::string>& lines, const std::string& verb,
                      const std::string& key) {
    int n = 0;
    for (const auto& l : lines) {
        const auto t = Tokens(l);
        if (t.size() >= 4 && t[0] == verb && t[3] == key) ++n;
    }
    return n;
}

// ---------- RaftGroupClient: kWrongShard ----------

void TestGroupClientWrongShardOnWrite() {
    std::cout << "group client: WRONG_SHARD on PUT is kWrongShard, version parsed, no retry\n";
    FakeLineServer server([](const std::string&) { return std::optional<std::string>("WRONG_SHARD 7"); });
    RaftGroupClient client({Ep(server.port())}, FastOptions());
    const auto r = client.Put("k", "v");
    CHECK(r.status == RaftGroupClient::Status::kWrongShard);
    CHECK(r.map_version == 7);
    CHECK(server.received().size() == 1);  // definitive answer: not retried
}

void TestGroupClientWrongShardOnDeleteAndGet() {
    std::cout << "group client: WRONG_SHARD on DELETE and GET\n";
    FakeLineServer server([](const std::string& line) -> std::optional<std::string> {
        if (line == "LEADER") return std::string("LEADER");
        return std::string("WRONG_SHARD 12");
    });
    RaftGroupClient client({Ep(server.port())}, FastOptions());
    const auto d = client.Delete("k");
    CHECK(d.status == RaftGroupClient::Status::kWrongShard && d.map_version == 12);
    const auto g = client.Get("k");
    CHECK(g.status == RaftGroupClient::Status::kWrongShard && g.map_version == 12);
}

void TestGroupClientWrongShardVariants() {
    std::cout << "group client: bare WRONG_SHARD -> version 0; garbage versions -> kError\n";
    {
        FakeLineServer s([](const std::string&) { return std::optional<std::string>("WRONG_SHARD"); });
        RaftGroupClient c({Ep(s.port())}, FastOptions());
        const auto r = c.Put("k", "v");
        CHECK(r.status == RaftGroupClient::Status::kWrongShard && r.map_version == 0);
    }
    for (const char* bad : {"WRONG_SHARD abc", "WRONG_SHARD 3x", "WRONG_SHARD -1",
                            "WRONG_SHARD 99999999999999999999", "WRONG_SHARD  4", "WRONG_SHARDX 4"}) {
        FakeLineServer s([bad](const std::string&) { return std::optional<std::string>(bad); });
        RaftGroupClient c({Ep(s.port())}, FastOptions());
        const auto r = c.Put("k", "v");
        CHECK(r.status == RaftGroupClient::Status::kError);
        CHECK(r.detail == bad);
    }
}

// ---------- ClientRouter ----------

void TestRoutesToOwningShard() {
    std::cout << "router: each key goes only to the shard the ring says owns it\n";
    FakeShard s0(OkEverything), s1(OkEverything);
    const ShardMap map = MakeMap(1, {{0, {s0.port()}}, {1, {s1.port()}}});
    FakeMetadata meta(EncodeChecked(map));
    ClientRouter router(meta.endpoints(), FastRouterOptions());

    const std::string k0 = KeysOwnedBy(map, 0, 1).at(0);
    const std::string k1 = KeysOwnedBy(map, 1, 1).at(0);

    CHECK(router.Put(k0, "a").status == RaftGroupClient::Status::kOk);  // lazy first fetch
    CHECK(router.Put(k1, "b").status == RaftGroupClient::Status::kOk);
    const auto g = router.Get(k1);
    CHECK(g.status == RaftGroupClient::Status::kOk && g.value == "v-" + k1);
    CHECK(router.Delete(k0).status == RaftGroupClient::Status::kOk);

    CHECK(CountLinesWithKey(s0.received(), "PUT", k0) == 1);
    CHECK(CountLinesWithKey(s0.received(), "DELETE", k0) == 1);
    CHECK(CountLinesWithKey(s1.received(), "PUT", k1) == 1);
    CHECK(CountLinesWithKey(s0.received(), "PUT", k1) == 0);
    CHECK(CountLinesWithKey(s1.received(), "PUT", k0) == 0);
    CHECK(meta.fetches == 1);  // one lazy fetch, then cached
    CHECK(router.map_version() == 1);
}

void TestStaleRouterRefetchesAndRetries() {
    std::cout << "router: WRONG_SHARD with NEWER version -> refetch, re-route, retry\n";
    FakeShard b(OkEverything);
    std::string moved_key;  // key that moves to shard 1 in v2
    FakeShard a([&](const std::string& line) {
        const auto t = Tokens(line);
        if (t.size() >= 4 && t[0] == "PUT" && t[3] == moved_key) return std::string("WRONG_SHARD 2");
        return OkEverything(line);
    });

    const ShardMap v1 = MakeMap(1, {{0, {a.port()}}});
    const ShardMap v2 = MakeMap(2, {{0, {a.port()}}, {1, {b.port()}}});
    moved_key = KeysOwnedBy(v2, 1, 1).at(0);
    const std::string stays_key = KeysOwnedBy(v2, 0, 1).at(0);

    FakeMetadata meta(EncodeChecked(v1));
    ClientRouter router(meta.endpoints(), FastRouterOptions());
    CHECK(router.Refresh().status == ClientRouter::RefreshStatus::kOk);
    CHECK(router.map_version() == 1);
    CHECK(router.Put(stays_key, "x").status == RaftGroupClient::Status::kOk);

    meta.Set(EncodeChecked(v2));  // topology changes behind the router's back

    const auto r = router.Put(moved_key, "y");
    CHECK(r.status == RaftGroupClient::Status::kOk);
    CHECK(router.map_version() == 2);
    CHECK(meta.fetches == 2);                              // initial + exactly one refetch
    CHECK(CountLinesWithKey(a.received(), "PUT", moved_key) == 1);  // tried the old owner once
    CHECK(CountLinesWithKey(b.received(), "PUT", moved_key) == 1);  // landed on the new owner

    // Shard 0's endpoints did not change, so its client (leader cache,
    // client_id, seq counter) must have been REUSED across the swap.
    CHECK(router.Put(stays_key, "z").status == RaftGroupClient::Status::kOk);
    std::vector<std::vector<std::string>> stays;
    for (const auto& l : a.received()) {
        const auto t = Tokens(l);
        if (t.size() >= 4 && t[0] == "PUT" && t[3] == stays_key) stays.push_back(t);
    }
    CHECK(stays.size() == 2);
    if (stays.size() == 2) {
        CHECK(stays[0][1] == stays[1][1]);                                // same client_id
        CHECK(std::stoull(stays[1][2]) > std::stoull(stays[0][2]));       // seq kept advancing
    }
}

void TestEqualVersionIsAnErrorNoRefetch() {
    std::cout << "router: WRONG_SHARD with version == cached -> error, no refetch\n";
    FakeShard a([](const std::string&) { return std::string("WRONG_SHARD 1"); });
    const ShardMap v1 = MakeMap(1, {{0, {a.port()}}});
    FakeMetadata meta(EncodeChecked(v1));
    ClientRouter router(meta.endpoints(), FastRouterOptions());
    const auto r = router.Put("k", "v");
    CHECK(r.status == RaftGroupClient::Status::kError);
    CHECK(r.detail.find("not newer") != std::string::npos);
    CHECK(meta.fetches == 1);
    CHECK(a.received().size() == 1);
}

void TestOlderVersionIsAnError() {
    std::cout << "router: WRONG_SHARD with OLDER (or missing) version -> error, no refetch\n";
    for (const char* reply : {"WRONG_SHARD 3", "WRONG_SHARD"}) {
        FakeShard a([reply](const std::string&) { return std::string(reply); });
        FakeMetadata meta(EncodeChecked(MakeMap(5, {{0, {a.port()}}})));
        ClientRouter router(meta.endpoints(), FastRouterOptions());
        const auto r = router.Get("k");
        CHECK(r.status == RaftGroupClient::Status::kError);
        CHECK(meta.fetches == 1);
    }
}

void TestNodeAheadOfMetadataIsAnError() {
    std::cout << "router: node claims newer map but metadata has nothing newer -> error after ONE refetch\n";
    FakeShard a([](const std::string&) { return std::string("WRONG_SHARD 5"); });
    FakeMetadata meta(EncodeChecked(MakeMap(1, {{0, {a.port()}}})));
    ClientRouter router(meta.endpoints(), FastRouterOptions());
    const auto r = router.Put("k", "v");
    CHECK(r.status == RaftGroupClient::Status::kError);
    CHECK(r.detail.find("no newer map") != std::string::npos);
    CHECK(meta.fetches == 2);
    CHECK(a.received().size() == 1);  // did not hammer the node again
}

void TestRetriesAreBounded() {
    std::cout << "router: a map that changes on every fetch cannot loop forever\n";
    FakeShard a([](const std::string&) { return std::string("WRONG_SHARD 100"); });
    FakeMetadata meta("");
    meta.supplier = [&](int n) {  // fetch #n returns version n: always "newer"
        return EncodeChecked(MakeMap(static_cast<std::uint64_t>(n), {{0, {a.port()}}}));
    };
    ClientRouter router(meta.endpoints(), FastRouterOptions(/*max_attempts=*/3));
    const auto r = router.Put("k", "v");
    CHECK(r.status == RaftGroupClient::Status::kError);
    CHECK(r.detail.find("gave up") != std::string::npos);
    CHECK(meta.fetches == 4);          // 1 lazy + 3 refetches
    CHECK(a.received().size() == 3);   // one PUT per attempt
}

void TestConcurrentCallersShareOneRefetch() {
    std::cout << "router: 8 concurrent callers hit a stale map -> exactly one refetch\n";
    FakeShard b(OkEverything);
    std::vector<std::string> moved;  // filled before threads start
    FakeShard a([&](const std::string& line) {
        const auto t = Tokens(line);
        if (t.size() >= 4 && t[0] == "PUT") return std::string("WRONG_SHARD 2");
        return OkEverything(line);
    });
    const ShardMap v1 = MakeMap(1, {{0, {a.port()}}});
    const ShardMap v2 = MakeMap(2, {{0, {a.port()}}, {1, {b.port()}}});
    moved = KeysOwnedBy(v2, 1, 8);
    CHECK(moved.size() == 8);

    FakeMetadata meta(EncodeChecked(v1));
    ClientRouter router(meta.endpoints(), FastRouterOptions());
    CHECK(router.Refresh().status == ClientRouter::RefreshStatus::kOk);
    meta.Set(EncodeChecked(v2));

    std::atomic<int> ok{0};
    std::vector<std::thread> threads;
    for (const auto& key : moved) {
        threads.emplace_back([&, key] {
            if (router.Put(key, "v").status == RaftGroupClient::Status::kOk) ++ok;
        });
    }
    for (auto& t : threads) t.join();

    CHECK(ok == 8);
    CHECK(meta.fetches == 2);  // initial + ONE shared refetch, not eight
    CHECK(router.map_version() == 2);
    int landed = 0;
    for (const auto& key : moved) landed += CountLinesWithKey(b.received(), "PUT", key);
    CHECK(landed == 8);
}

void TestNoMapPublished() {
    std::cout << "router: nothing published -> Refresh says so, operations are kUnavailable\n";
    FakeMetadata meta("");
    meta.raw_reply_override = "NOT_FOUND";
    ClientRouter router(meta.endpoints(), FastRouterOptions());
    CHECK(router.Refresh().status == ClientRouter::RefreshStatus::kNoMapPublished);
    const auto r = router.Put("k", "v");
    CHECK(r.status == RaftGroupClient::Status::kUnavailable);
    CHECK(r.detail.find("no shard map") != std::string::npos);
    CHECK(router.map_version() == 0);
}

void TestCorruptMapRejected() {
    std::cout << "router: corrupt map in the metadata group is rejected, nothing installed\n";
    FakeMetadata meta("");
    meta.raw_reply_override = "VALUE SM1;v=3;vn=64;s=0@x/y;ck=0000000000000000";
    ClientRouter router(meta.endpoints(), FastRouterOptions());
    CHECK(router.Refresh().status == ClientRouter::RefreshStatus::kBadMap);
    CHECK(router.Put("k", "v").status == RaftGroupClient::Status::kUnavailable);
    CHECK(router.map_version() == 0);
}

void TestBadClientAddressRejected() {
    std::cout << "router: a node client_addr that is not host:port -> kBadMap\n";
    ShardMap m = MakeMap(1, {{0, {1234}}});
    m.shards[0].nodes[0].client_addr = "not-an-address";
    FakeMetadata meta(EncodeChecked(m));
    ClientRouter router(meta.endpoints(), FastRouterOptions());
    CHECK(router.Refresh().status == ClientRouter::RefreshStatus::kBadMap);
    CHECK(router.map_version() == 0);
}

void TestCacheNeverGoesBackwards() {
    std::cout << "router: an older map from the metadata group never replaces a newer cached one\n";
    FakeShard a(OkEverything);
    FakeMetadata meta(EncodeChecked(MakeMap(2, {{0, {a.port()}}})));
    ClientRouter router(meta.endpoints(), FastRouterOptions());
    CHECK(router.Refresh().version == 2);
    meta.Set(EncodeChecked(MakeMap(1, {{0, {a.port()}}})));
    const auto r = router.Refresh();
    CHECK(r.status == ClientRouter::RefreshStatus::kOk);
    CHECK(r.version == 2);
    CHECK(router.map_version() == 2);
}

void TestMetadataUnreachable() {
    std::cout << "router: metadata group unreachable -> kUnavailable, bounded time\n";
    ClientRouterOptions o = FastRouterOptions();
    o.metadata_options.attempt_timeout_ms = 100;
    o.metadata_options.overall_timeout_ms = 300;
    ClientRouter router({Ep(UnusedPort())}, o);
    CHECK(router.Refresh().status == ClientRouter::RefreshStatus::kUnavailable);
    CHECK(router.Put("k", "v").status == RaftGroupClient::Status::kUnavailable);
}

}  // namespace

int main() {
    TestGroupClientWrongShardOnWrite();
    TestGroupClientWrongShardOnDeleteAndGet();
    TestGroupClientWrongShardVariants();

    TestRoutesToOwningShard();
    TestStaleRouterRefetchesAndRetries();
    TestEqualVersionIsAnErrorNoRefetch();
    TestOlderVersionIsAnError();
    TestNodeAheadOfMetadataIsAnError();
    TestRetriesAreBounded();
    TestConcurrentCallersShareOneRefetch();
    TestNoMapPublished();
    TestCorruptMapRejected();
    TestBadClientAddressRejected();
    TestCacheNeverGoesBackwards();
    TestMetadataUnreachable();

    if (g_failures == 0) {
        std::cout << "ALL PASSED\n";
        return 0;
    }
    std::cout << g_failures << " CHECK(S) FAILED\n";
    return 1;
}