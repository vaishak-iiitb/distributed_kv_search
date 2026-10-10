#include <cstdio>
#include <map>
#include <mutex>
#include <string>

#include "fake_line_server.h"
#include "sharding/metadata_client.h"

namespace {

int g_failures = 0;

#define CHECK(cond)                                                                    \
    do {                                                                               \
        if (!(cond)) {                                                                 \
            std::fprintf(stderr, "%s:%d: CHECK failed: %s\n", __FILE__, __LINE__, #cond); \
            ++g_failures;                                                              \
        }                                                                              \
    } while (0)

using sharding::Endpoint;
using sharding::MetadataClient;
using sharding::RaftGroupClientOptions;

// A syntactically plausible encoded map. The checksum is not real: the
// metadata client only reads the version, by design.
std::string FakeMap(std::uint64_t version) {
    return "SM1;v=" + std::to_string(version) + ";vn=64;s=0@127.0.0.1:1/127.0.0.1:2;ck=00";
}

RaftGroupClientOptions FastOptions() {
    RaftGroupClientOptions o;
    o.attempt_timeout_ms = 300;
    o.overall_timeout_ms = 800;
    o.retry_backoff_ms = 10;
    o.client_id = 5;
    return o;
}

// A one-key in-memory stand-in for a metadata group's leader, speaking the
// real ClientServer line protocol (LEADER / GET / PUT).
struct FakeMetadataLeader {
    std::mutex mu;
    std::map<std::string, std::string> store;

    std::optional<std::string> Handle(const std::string& line) {
        std::lock_guard<std::mutex> lock(mu);
        if (line == "LEADER") return "LEADER";
        if (line.rfind("GET ", 0) == 0) {
            const auto it = store.find(line.substr(4));
            return it == store.end() ? "NOT_FOUND" : "VALUE " + it->second;
        }
        if (line.rfind("PUT ", 0) == 0) {
            // PUT <cid> <seq> <key> <value...>
            std::size_t p1 = line.find(' ', 4);
            std::size_t p2 = line.find(' ', p1 + 1);
            std::size_t p3 = line.find(' ', p2 + 1);
            store[line.substr(p2 + 1, p3 - p2 - 1)] = line.substr(p3 + 1);
            return "OK";
        }
        return "ERROR unknown command";
    }
};

// ------------------------------------------------------ ParseShardMapVersion

void test_parse_version_accepts_valid() {
    std::uint64_t v = 0;
    CHECK(sharding::ParseShardMapVersion("SM1;v=7;vn=64;s=0@a:1/b:2;ck=ab", &v) && v == 7);
    CHECK(sharding::ParseShardMapVersion("SM1;v=0;x=1", &v) && v == 0);
    CHECK(sharding::ParseShardMapVersion("SM1;v=18446744073709551615", &v) &&
          v == 18446744073709551615ull);
}

void test_parse_version_does_not_confuse_vn() {
    std::uint64_t v = 0;
    CHECK(sharding::ParseShardMapVersion("SM1;vn=64;v=3;ck=00", &v) && v == 3);
    CHECK(!sharding::ParseShardMapVersion("SM1;vn=64;ck=00", &v));  // no v= at all
}

void test_parse_version_rejects_garbage() {
    std::uint64_t v = 0;
    CHECK(!sharding::ParseShardMapVersion("", &v));
    CHECK(!sharding::ParseShardMapVersion("SM2;v=1", &v));         // wrong magic
    CHECK(!sharding::ParseShardMapVersion("v=1;SM1", &v));         // not at the front
    CHECK(!sharding::ParseShardMapVersion("SM1;v=;x=1", &v));      // empty
    CHECK(!sharding::ParseShardMapVersion("SM1;v=12x;x=1", &v));   // trailing junk
    CHECK(!sharding::ParseShardMapVersion("SM1;v=-1", &v));        // sign
    CHECK(!sharding::ParseShardMapVersion("SM1;v=18446744073709551616", &v));  // overflow
}

// ------------------------------------------------------------ MetadataClient

void test_fetch_empty_then_publish_then_fetch() {
    FakeMetadataLeader meta;
    FakeLineServer server([&meta](const std::string& l) { return meta.Handle(l); });
    MetadataClient client({Endpoint{"127.0.0.1", server.port()}}, FastOptions());

    CHECK(client.Fetch().status == MetadataClient::FetchStatus::kEmpty);

    const auto pub = client.Publish(FakeMap(1));
    CHECK(pub.status == MetadataClient::PublishStatus::kOk);

    const auto fetched = client.Fetch();
    CHECK(fetched.status == MetadataClient::FetchStatus::kOk);
    CHECK(fetched.version == 1);
    CHECK(fetched.encoded_map == FakeMap(1));  // byte-exact round trip
}

void test_publish_requires_strictly_greater_version() {
    FakeMetadataLeader meta;
    FakeLineServer server([&meta](const std::string& l) { return meta.Handle(l); });
    MetadataClient client({Endpoint{"127.0.0.1", server.port()}}, FastOptions());

    CHECK(client.Publish(FakeMap(2)).status == MetadataClient::PublishStatus::kOk);
    CHECK(client.Publish(FakeMap(3)).status == MetadataClient::PublishStatus::kOk);

    const auto older = client.Publish(FakeMap(1));
    CHECK(older.status == MetadataClient::PublishStatus::kStaleVersion);
    CHECK(older.current_version == 3);

    const auto same = client.Publish(FakeMap(3));  // equal is also rejected
    CHECK(same.status == MetadataClient::PublishStatus::kStaleVersion);

    CHECK(client.Fetch().version == 3);  // nothing was overwritten
}

void test_publish_malformed_sends_no_put() {
    FakeMetadataLeader meta;
    FakeLineServer server([&meta](const std::string& l) { return meta.Handle(l); });
    MetadataClient client({Endpoint{"127.0.0.1", server.port()}}, FastOptions());

    CHECK(client.Publish("garbage").status == MetadataClient::PublishStatus::kMalformed);
    CHECK(server.received().empty());  // rejected locally, before any network
}

void test_corrupt_stored_value_is_reported_not_overwritten() {
    FakeMetadataLeader meta;
    meta.store[sharding::kShardMapKey] = "not-a-shard-map";
    FakeLineServer server([&meta](const std::string& l) { return meta.Handle(l); });
    MetadataClient client({Endpoint{"127.0.0.1", server.port()}}, FastOptions());

    CHECK(client.Fetch().status == MetadataClient::FetchStatus::kCorrupt);
    CHECK(client.Publish(FakeMap(9)).status == MetadataClient::PublishStatus::kCorruptStored);
    CHECK(meta.store[sharding::kShardMapKey] == "not-a-shard-map");  // untouched
}

void test_unavailable_group() {
    MetadataClient client({Endpoint{"127.0.0.1", UnusedPort()}}, FastOptions());
    CHECK(client.Fetch().status == MetadataClient::FetchStatus::kUnavailable);
    CHECK(client.Publish(FakeMap(1)).status == MetadataClient::PublishStatus::kUnavailable);
}

}  // namespace

int main() {
    test_parse_version_accepts_valid();
    test_parse_version_does_not_confuse_vn();
    test_parse_version_rejects_garbage();
    test_fetch_empty_then_publish_then_fetch();
    test_publish_requires_strictly_greater_version();
    test_publish_malformed_sends_no_put();
    test_corrupt_stored_value_is_reported_not_overwritten();
    test_unavailable_group();

    if (g_failures == 0) {
        std::printf("test_metadata_client: all checks passed\n");
        return 0;
    }
    std::fprintf(stderr, "test_metadata_client: %d check(s) failed\n", g_failures);
    return 1;
}