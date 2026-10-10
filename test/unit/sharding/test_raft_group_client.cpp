#include <atomic>
#include <chrono>
#include <cstdio>
#include <string>
#include <vector>

#include "fake_line_server.h"
#include "net/line_client.h"
#include "sharding/raft_group_client.h"

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

long ElapsedMs(Clock::time_point since) {
    return std::chrono::duration_cast<std::chrono::milliseconds>(Clock::now() - since).count();
}

Endpoint Local(int port) { return Endpoint{"127.0.0.1", port}; }

// Short budgets so failure-path tests stay fast.
RaftGroupClientOptions FastOptions(std::uint64_t client_id = 7) {
    RaftGroupClientOptions o;
    o.attempt_timeout_ms = 300;
    o.overall_timeout_ms = 1500;
    o.retry_backoff_ms = 10;
    o.client_id = client_id;
    return o;
}

// ---------------------------------------------------------------- LineClient

void test_line_client_round_trip_and_crlf() {
    FakeLineServer server([](const std::string& line) -> std::optional<std::string> {
        return "echo:" + line + "\r";  // the client must strip the trailing \r
    });
    const auto r = net::LineClient::RoundTrip("127.0.0.1", server.port(), "hello", 1000);
    CHECK(r.has_value() && *r == "echo:hello");
}

void test_line_client_refused_is_fast_nullopt() {
    const auto start = Clock::now();
    const auto r = net::LineClient::RoundTrip("127.0.0.1", UnusedPort(), "x", 2000);
    CHECK(!r.has_value());
    CHECK(ElapsedMs(start) < 1000);  // refusal must not wait out the timeout
}

void test_line_client_silent_server_times_out_on_time() {
    FakeLineServer server([](const std::string&) -> std::optional<std::string> { return std::nullopt; });
    const auto start = Clock::now();
    const auto r = net::LineClient::RoundTrip("127.0.0.1", server.port(), "x", 300);
    const long ms = ElapsedMs(start);
    CHECK(!r.has_value());
    CHECK(ms >= 250 && ms < 1000);
}

void test_line_client_rejects_unparseable_host() {
    CHECK(!net::LineClient::RoundTrip("not-an-ip", 1234, "x", 200).has_value());
}

// ------------------------------------------------------------- ParseEndpoint

void test_parse_endpoint() {
    Endpoint e;
    CHECK(sharding::ParseEndpoint("127.0.0.1:8080", &e) && e.host == "127.0.0.1" && e.port == 8080);
    CHECK(sharding::ParseEndpoint("localhost:1", &e) && e.port == 1);
    CHECK(sharding::ParseEndpoint("h:65535", &e) && e.port == 65535);
    CHECK(!sharding::ParseEndpoint("127.0.0.1", &e));
    CHECK(!sharding::ParseEndpoint(":80", &e));
    CHECK(!sharding::ParseEndpoint("h:", &e));
    CHECK(!sharding::ParseEndpoint("h:0", &e));
    CHECK(!sharding::ParseEndpoint("h:65536", &e));
    CHECK(!sharding::ParseEndpoint("h:80x", &e));
    CHECK(!sharding::ParseEndpoint("h:-1", &e));
    CHECK(!sharding::ParseEndpoint("h:123456", &e));
}

// ------------------------------------------------------------ RaftGroupClient

void test_put_delete_wire_format_and_sequence_numbers() {
    FakeLineServer server([](const std::string&) -> std::optional<std::string> { return "OK"; });
    RaftGroupClient client({Local(server.port())}, FastOptions(42));

    CHECK(client.Put("k", "hello world").status == RaftGroupClient::Status::kOk);
    CHECK(client.Put("k2", "v2").status == RaftGroupClient::Status::kOk);
    CHECK(client.Delete("k").status == RaftGroupClient::Status::kOk);

    const auto got = server.received();
    CHECK(got.size() == 3);
    if (got.size() == 3) {
        CHECK(got[0] == "PUT 42 1 k hello world");
        CHECK(got[1] == "PUT 42 2 k2 v2");
        CHECK(got[2] == "DELETE 42 3 k");
    }
}

void test_redirect_is_followed_even_to_unconfigured_address() {
    FakeLineServer leader([](const std::string&) -> std::optional<std::string> { return "OK"; });
    const std::string hint = "NOT_LEADER 127.0.0.1:" + std::to_string(leader.port());
    FakeLineServer follower([hint](const std::string&) -> std::optional<std::string> { return hint; });

    // Only the follower is configured; the leader is learned from the hint.
    RaftGroupClient client({Local(follower.port())}, FastOptions());
    CHECK(client.Put("k", "v").status == RaftGroupClient::Status::kOk);

    CHECK(follower.received().size() == 1);
    CHECK(leader.received().size() == 1);
    CHECK(follower.received() == leader.received());  // identical line, same seq

    // The leader is now cached: the next write goes straight to it.
    CHECK(client.Put("k2", "v2").status == RaftGroupClient::Status::kOk);
    CHECK(follower.received().size() == 1);
    CHECK(leader.received().size() == 2);
}

void test_timeout_is_retried_with_identical_line() {
    std::atomic<int> calls{0};
    FakeLineServer server([&calls](const std::string&) -> std::optional<std::string> {
        return calls++ == 0 ? "TIMEOUT" : "OK";
    });
    RaftGroupClient client({Local(server.port())}, FastOptions(9));

    CHECK(client.Put("k", "v").status == RaftGroupClient::Status::kOk);
    const auto got = server.received();
    CHECK(got.size() == 2);
    if (got.size() == 2) {
        CHECK(got[0] == got[1]);  // SAME (client_id, seq): the idempotency contract
        CHECK(got[0] == "PUT 9 1 k v");
    }

    // And the NEXT operation moves on to a fresh sequence number.
    CHECK(client.Put("k", "v2").status == RaftGroupClient::Status::kOk);
    CHECK(server.received().back() == "PUT 9 2 k v2");
}

void test_skips_dead_endpoint() {
    FakeLineServer live([](const std::string&) -> std::optional<std::string> { return "OK"; });
    RaftGroupClient client({Local(UnusedPort()), Local(live.port())}, FastOptions());
    CHECK(client.Put("k", "v").status == RaftGroupClient::Status::kOk);
    CHECK(live.received().size() == 1);
}

void test_bare_not_leader_rotates_to_next_endpoint() {
    FakeLineServer no_idea([](const std::string&) -> std::optional<std::string> { return "NOT_LEADER"; });
    FakeLineServer leader([](const std::string&) -> std::optional<std::string> { return "OK"; });
    RaftGroupClient client({Local(no_idea.port()), Local(leader.port())}, FastOptions());
    CHECK(client.Put("k", "v").status == RaftGroupClient::Status::kOk);
}

void test_everything_down_reports_unavailable_within_budget() {
    RaftGroupClientOptions o = FastOptions();
    o.overall_timeout_ms = 400;
    RaftGroupClient client({Local(UnusedPort()), Local(UnusedPort())}, o);
    const auto start = Clock::now();
    const auto r = client.Put("k", "v");
    CHECK(r.status == RaftGroupClient::Status::kUnavailable);
    CHECK(!r.detail.empty());
    CHECK(ElapsedMs(start) < 1200);
}

void test_redirect_cycle_backs_off_instead_of_spinning() {
    // Two nodes that each insist the OTHER is leader (a stale-view loop).
    std::atomic<int> b_port{0};
    FakeLineServer a([&b_port](const std::string&) -> std::optional<std::string> {
        return "NOT_LEADER 127.0.0.1:" + std::to_string(b_port.load());
    });
    std::atomic<int> a_port{a.port()};
    FakeLineServer b([&a_port](const std::string&) -> std::optional<std::string> {
        return "NOT_LEADER 127.0.0.1:" + std::to_string(a_port.load());
    });
    b_port = b.port();

    RaftGroupClientOptions o = FastOptions();
    o.overall_timeout_ms = 500;
    o.retry_backoff_ms = 50;
    RaftGroupClient client({Local(a.port()), Local(b.port())}, o);
    const auto r = client.Put("k", "v");
    CHECK(r.status == RaftGroupClient::Status::kUnavailable);
    // With backoff every lap, 500ms allows only a modest number of requests.
    // A spinning loop would send thousands.
    CHECK(a.received().size() + b.received().size() < 60);
}

void test_server_error_is_not_retried() {
    FakeLineServer server([](const std::string&) -> std::optional<std::string> { return "ERROR malformed PUT"; });
    RaftGroupClient client({Local(server.port())}, FastOptions());
    const auto r = client.Put("k", "v");
    CHECK(r.status == RaftGroupClient::Status::kError);
    CHECK(server.received().size() == 1);
}

void test_argument_validation_sends_nothing() {
    FakeLineServer server([](const std::string&) -> std::optional<std::string> { return "OK"; });
    RaftGroupClient client({Local(server.port())}, FastOptions());
    CHECK(client.Put("", "v").status == RaftGroupClient::Status::kError);
    CHECK(client.Put("has space", "v").status == RaftGroupClient::Status::kError);
    CHECK(client.Put("k", "line\nbreak").status == RaftGroupClient::Status::kError);
    CHECK(client.Delete("").status == RaftGroupClient::Status::kError);
    CHECK(client.Get("bad key").status == RaftGroupClient::Status::kError);
    CHECK(server.received().empty());
}

void test_get_probes_leader_first_then_reads() {
    FakeLineServer server([](const std::string& line) -> std::optional<std::string> {
        if (line == "LEADER") return "LEADER";
        if (line == "GET present") return "VALUE hello world";
        if (line == "GET empty") return "VALUE ";
        return "NOT_FOUND";
    });
    RaftGroupClient client({Local(server.port())}, FastOptions());

    const auto a = client.Get("present");
    CHECK(a.status == RaftGroupClient::Status::kOk && a.value == "hello world");
    const auto b = client.Get("empty");
    CHECK(b.status == RaftGroupClient::Status::kOk && b.value.empty());
    const auto c = client.Get("missing");
    CHECK(c.status == RaftGroupClient::Status::kNotFound);

    const auto got = server.received();
    CHECK(got.size() == 6);  // every Get = LEADER probe + GET
    if (got.size() == 6) {
        CHECK(got[0] == "LEADER" && got[1] == "GET present");
        CHECK(got[2] == "LEADER" && got[3] == "GET empty");
    }
}

void test_get_follows_leader_hint_and_never_reads_from_follower() {
    FakeLineServer leader([](const std::string& line) -> std::optional<std::string> {
        if (line == "LEADER") return "LEADER";
        return "VALUE fresh";
    });
    const std::string hint = "NOT_LEADER 127.0.0.1:" + std::to_string(leader.port());
    FakeLineServer follower([hint](const std::string& line) -> std::optional<std::string> {
        if (line == "LEADER") return hint;
        return "VALUE STALE";  // would be wrong to ever read from here
    });

    RaftGroupClient client({Local(follower.port())}, FastOptions());
    const auto r = client.Get("k");
    CHECK(r.status == RaftGroupClient::Status::kOk && r.value == "fresh");
    CHECK(follower.received().size() == 1 && follower.received()[0] == "LEADER");
}

void test_two_clients_get_different_random_ids() {
    FakeLineServer server([](const std::string&) -> std::optional<std::string> { return "OK"; });
    RaftGroupClientOptions o = FastOptions();
    o.client_id = 0;  // random
    RaftGroupClient a({Local(server.port())}, o);
    RaftGroupClient b({Local(server.port())}, o);
    CHECK(a.client_id() != 0 && b.client_id() != 0);
    CHECK(a.client_id() != b.client_id());
    CHECK(a.client_id() < 0x80000000ull && b.client_id() < 0x80000000ull);
}

}  // namespace

int main() {
    test_line_client_round_trip_and_crlf();
    test_line_client_refused_is_fast_nullopt();
    test_line_client_silent_server_times_out_on_time();
    test_line_client_rejects_unparseable_host();
    test_parse_endpoint();
    test_put_delete_wire_format_and_sequence_numbers();
    test_redirect_is_followed_even_to_unconfigured_address();
    test_timeout_is_retried_with_identical_line();
    test_skips_dead_endpoint();
    test_bare_not_leader_rotates_to_next_endpoint();
    test_everything_down_reports_unavailable_within_budget();
    test_redirect_cycle_backs_off_instead_of_spinning();
    test_server_error_is_not_retried();
    test_argument_validation_sends_nothing();
    test_get_probes_leader_first_then_reads();
    test_get_follows_leader_hint_and_never_reads_from_follower();
    test_two_clients_get_different_random_ids();

    if (g_failures == 0) {
        std::printf("test_raft_group_client: all checks passed\n");
        return 0;
    }
    std::fprintf(stderr, "test_raft_group_client: %d check(s) failed\n", g_failures);
    return 1;
}