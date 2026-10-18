#include "sharding/raft_group_client.h"

#include <algorithm>
#include <chrono>
#include <cstdlib>
#include <random>
#include <thread>

#include "net/line_client.h"

namespace sharding {
namespace {

using Clock = std::chrono::steady_clock;

bool HasPrefix(const std::string& s, const std::string& prefix) {
    return s.compare(0, prefix.size(), prefix) == 0;
}

bool KeyIsValid(const std::string& key) {
    if (key.empty()) return false;
    return std::none_of(key.begin(), key.end(), [](char c) {
        return c == ' ' || c == '\t' || c == '\n' || c == '\r';
    });
}

bool ValueIsValid(const std::string& value) {
    return value.find('\n') == std::string::npos && value.find('\r') == std::string::npos;
}

// Client ids are random. They must NOT repeat across client restarts: the
// server dedups by (client_id, seq), so a restarted client that reused an
// id and restarted its seq at 1 would have every write silently treated as
// a duplicate. Kept below 2^31 in case ClientId is narrower than 64 bits
// (check `grep ClientId src/raft/command.h`).
std::uint64_t RandomClientId() {
    std::random_device rd;
    std::mt19937_64 gen((static_cast<std::uint64_t>(rd()) << 32) ^ rd() ^
                        static_cast<std::uint64_t>(Clock::now().time_since_epoch().count()));
    return (gen() % 0x7FFFFFFEull) + 1;  // in [1, 2^31 - 1)
}

int RemainingMs(Clock::time_point deadline) {
    const auto left =
        std::chrono::duration_cast<std::chrono::milliseconds>(deadline - Clock::now()).count();
    return left < 0 ? 0 : static_cast<int>(left);
}

void SleepUpTo(int ms, Clock::time_point deadline) {
    const int pause = std::min(ms, RemainingMs(deadline));
    if (pause > 0) std::this_thread::sleep_for(std::chrono::milliseconds(pause));
}

// The endpoint after `current` in the configured list (wrapping). If
// `current` is not in the list (a redirect hint to an address we weren't
// configured with), start from the beginning.
Endpoint NextEndpoint(const std::vector<Endpoint>& endpoints, const Endpoint& current) {
    for (std::size_t i = 0; i < endpoints.size(); ++i) {
        if (endpoints[i] == current) return endpoints[(i + 1) % endpoints.size()];
    }
    return endpoints.front();
}

// One request/response with the per-attempt timeout clipped to the budget.
std::optional<std::string> Ask(const Endpoint& target, const std::string& line,
                               int attempt_timeout_ms, Clock::time_point deadline) {
    const int budget = std::min(attempt_timeout_ms, RemainingMs(deadline));
    if (budget <= 0) return std::nullopt;
    return net::LineClient::RoundTrip(target.host, target.port, line, budget);
}

// Parses "NOT_LEADER" / "NOT_LEADER host:port". Returns true and fills
// `hint` only when a valid address follows.
bool ParseRedirect(const std::string& response, Endpoint* hint) {
    if (!HasPrefix(response, "NOT_LEADER ")) return false;
    return ParseEndpoint(response.substr(std::string("NOT_LEADER ").size()), hint);
}

// Parses "WRONG_SHARD" or "WRONG_SHARD <version>". Returns false if the line
// is not a WRONG_SHARD reply at all, or carries a version that is not a
// plain decimal number (then it is an unknown reply -> kError, not a
// guess). A bare "WRONG_SHARD" yields version 0.
bool ParseWrongShard(const std::string& response, std::uint64_t* version) {
    static const std::string kTag = "WRONG_SHARD";
    if (response == kTag) {
        *version = 0;
        return true;
    }
    if (!HasPrefix(response, kTag + " ")) return false;

    const std::string digits = response.substr(kTag.size() + 1);
    if (digits.empty() || digits.size() > 19) return false;  // 19 digits cannot overflow uint64
    std::uint64_t v = 0;
    for (char c : digits) {
        if (c < '0' || c > '9') return false;
        v = v * 10 + static_cast<std::uint64_t>(c - '0');
    }
    *version = v;
    return true;
}

}  // namespace

bool ParseEndpoint(const std::string& text, Endpoint* out) {
    const std::size_t colon = text.rfind(':');
    if (colon == std::string::npos || colon == 0 || colon + 1 >= text.size()) return false;

    const std::string port_text = text.substr(colon + 1);
    if (port_text.size() > 5) return false;
    int port = 0;
    for (char c : port_text) {
        if (c < '0' || c > '9') return false;
        port = port * 10 + (c - '0');
    }
    if (port < 1 || port > 65535) return false;

    out->host = text.substr(0, colon);
    out->port = port;
    return true;
}

RaftGroupClient::RaftGroupClient(std::vector<Endpoint> endpoints, RaftGroupClientOptions options)
    : endpoints_(std::move(endpoints)),
      options_(options),
      client_id_(options.client_id != 0 ? options.client_id : RandomClientId()) {}

RaftGroupClient::Result RaftGroupClient::Put(const std::string& key, const std::string& value) {
    if (!KeyIsValid(key)) return {Status::kError, "", "invalid key (empty or contains whitespace)"};
    if (!ValueIsValid(value)) return {Status::kError, "", "invalid value (contains a newline)"};

    std::lock_guard<std::mutex> lock(mu_);
    const std::uint64_t seq = next_seq_++;
    return SendWrite("PUT " + std::to_string(client_id_) + " " + std::to_string(seq) + " " + key +
                     " " + value);
}

RaftGroupClient::Result RaftGroupClient::Delete(const std::string& key) {
    if (!KeyIsValid(key)) return {Status::kError, "", "invalid key (empty or contains whitespace)"};

    std::lock_guard<std::mutex> lock(mu_);
    const std::uint64_t seq = next_seq_++;
    return SendWrite("DELETE " + std::to_string(client_id_) + " " + std::to_string(seq) + " " + key);
}

// Precondition: mu_ held. `line` already carries this operation's (client_id,
// seq); every retry below re-sends the IDENTICAL line, which is exactly what
// makes retrying an ambiguous outcome safe.
RaftGroupClient::Result RaftGroupClient::SendWrite(const std::string& line) {
    if (endpoints_.empty()) return {Status::kError, "", "no endpoints configured"};

    const auto deadline = Clock::now() + std::chrono::milliseconds(options_.overall_timeout_ms);
    Endpoint target = leader_.value_or(endpoints_.front());
    std::string last_detail = "no attempt made";
    std::size_t consecutive_redirects = 0;

    while (RemainingMs(deadline) > 0) {
        const auto response = Ask(target, line, options_.attempt_timeout_ms, deadline);

        if (!response.has_value()) {
            // Unreachable, or silent for the whole attempt timeout.
            last_detail = "no response from " + target.ToString();
            leader_.reset();
            target = NextEndpoint(endpoints_, target);
            consecutive_redirects = 0;
            SleepUpTo(options_.retry_backoff_ms, deadline);
            continue;
        }

        if (*response == "OK") {
            leader_ = target;
            return {Status::kOk, "", ""};
        }

        if (HasPrefix(*response, "NOT_LEADER")) {
            leader_.reset();
            last_detail = target.ToString() + " is not the leader";
            Endpoint hint;
            if (ParseRedirect(*response, &hint) && !(hint == target)) {
                target = hint;
                // Follow a hint immediately — but if hints keep chasing each
                // other around (stale views mid-election), back off once we
                // have made a full lap, instead of hammering the cluster.
                if (++consecutive_redirects >= std::max<std::size_t>(endpoints_.size(), 2)) {
                    consecutive_redirects = 0;
                    SleepUpTo(options_.retry_backoff_ms, deadline);
                }
            } else {
                target = NextEndpoint(endpoints_, target);
                consecutive_redirects = 0;
                SleepUpTo(options_.retry_backoff_ms, deadline);
            }
            continue;
        }

        if (*response == "TIMEOUT") {
            // AMBIGUOUS: the entry was proposed but not seen committing. It
            // may or may not have committed. Resend the same (client_id, seq)
            // line: if it did commit, the server's dedup makes this a no-op.
            last_detail = "commit timeout at " + target.ToString();
            consecutive_redirects = 0;
            SleepUpTo(options_.retry_backoff_ms, deadline);
            continue;
        }

        // The node is alive and decided it does not own this key. It rejected
        // the request BEFORE proposing it, so the write definitely did not
        // happen. Retrying here cannot help; the caller must re-route.
        std::uint64_t wrong_shard_version = 0;
        if (ParseWrongShard(*response, &wrong_shard_version)) {
            return {Status::kWrongShard, "", *response, wrong_shard_version};
        }

        // ERROR ..., or something we do not understand: retrying cannot help.
        return {Status::kError, "", *response};
    }

    return {Status::kUnavailable, "", last_detail};
}

RaftGroupClient::Result RaftGroupClient::Get(const std::string& key) {
    if (!KeyIsValid(key)) return {Status::kError, "", "invalid key (empty or contains whitespace)"};
    if (endpoints_.empty()) return {Status::kError, "", "no endpoints configured"};

    std::lock_guard<std::mutex> lock(mu_);
    const auto deadline = Clock::now() + std::chrono::milliseconds(options_.overall_timeout_ms);
    Endpoint target = leader_.value_or(endpoints_.front());
    std::string last_detail = "no attempt made";
    std::size_t consecutive_redirects = 0;

    while (RemainingMs(deadline) > 0) {
        // Always probe first, even for a cached leader: a deposed leader
        // that is still alive would happily answer GET from stale local
        // state. The probe narrows that window; it cannot close it (flag #2).
        const auto probe = Ask(target, "LEADER", options_.attempt_timeout_ms, deadline);

        if (!probe.has_value()) {
            last_detail = "no response from " + target.ToString();
            leader_.reset();
            target = NextEndpoint(endpoints_, target);
            consecutive_redirects = 0;
            SleepUpTo(options_.retry_backoff_ms, deadline);
            continue;
        }

        if (*probe != "LEADER") {
            leader_.reset();
            last_detail = target.ToString() + " is not the leader";
            Endpoint hint;
            if (ParseRedirect(*probe, &hint) && !(hint == target)) {
                target = hint;
                if (++consecutive_redirects >= std::max<std::size_t>(endpoints_.size(), 2)) {
                    consecutive_redirects = 0;
                    SleepUpTo(options_.retry_backoff_ms, deadline);
                }
            } else {
                target = NextEndpoint(endpoints_, target);
                consecutive_redirects = 0;
                SleepUpTo(options_.retry_backoff_ms, deadline);
            }
            continue;
        }

        leader_ = target;
        const auto response = Ask(target, "GET " + key, options_.attempt_timeout_ms, deadline);
        if (!response.has_value()) {
            last_detail = "leader " + target.ToString() + " went silent during GET";
            leader_.reset();
            SleepUpTo(options_.retry_backoff_ms, deadline);
            continue;
        }

        if (*response == "NOT_FOUND") return {Status::kNotFound, "", ""};
        if (*response == "VALUE" || HasPrefix(*response, "VALUE ")) {
            const std::string value =
                response->size() > 6 ? response->substr(6) : std::string();
            return {Status::kOk, value, ""};
        }
        // The node answered LEADER a moment ago but cannot serve the read yet
        // (its election no-op has not committed: TIMEOUT) or has just lost
        // leadership (NOT_LEADER). Neither is final: probe again.
        if (*response == "TIMEOUT" || HasPrefix(*response, "NOT_LEADER")) {
            last_detail = "leader " + target.ToString() + " not ready to serve reads: " + *response;
            leader_.reset();
            consecutive_redirects = 0;
            SleepUpTo(options_.retry_backoff_ms, deadline);
            continue;
        }
        std::uint64_t wrong_shard_version = 0;
        if (ParseWrongShard(*response, &wrong_shard_version)) {
            return {Status::kWrongShard, "", *response, wrong_shard_version};
        }
        return {Status::kError, "", *response};
    }

    return {Status::kUnavailable, "", last_detail};
}

}  // namespace sharding