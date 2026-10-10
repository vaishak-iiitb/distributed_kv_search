#pragma once

#include <cstdint>
#include <mutex>
#include <optional>
#include <string>
#include <vector>

namespace sharding {

// A client-facing address of one node in a Raft group (the port ClientServer
// listens on — NOT the Raft-to-Raft port).
struct Endpoint {
    std::string host;
    int port = 0;

    bool operator==(const Endpoint& o) const { return host == o.host && port == o.port; }
    std::string ToString() const { return host + ":" + std::to_string(port); }
};

// Parses "host:port". Strict: non-empty host, port in [1, 65535], digits only.
bool ParseEndpoint(const std::string& text, Endpoint* out);

struct RaftGroupClientOptions {
    // Must exceed ClientServer's propose_timeout (2000ms by default), or the
    // client gives up on a request the server is still legitimately waiting on.
    int attempt_timeout_ms = 3000;
    // Total budget for one operation, across all retries and redirects.
    int overall_timeout_ms = 10000;
    // Pause between retries that are NOT immediate redirect-follows.
    int retry_backoff_ms = 50;
    // 0 = pick a random id. Tests pin it to make assertions about wire lines.
    std::uint64_t client_id = 0;
};

// Talks to ONE Raft group through its ClientServers: finds the leader,
// follows NOT_LEADER redirects, retries ambiguous outcomes with the SAME
// sequence number (safe: the server dedups by (client_id, seq) — Step 9a).
// Used for the metadata group now, and for each data shard in the ClientRouter.
//
// Concurrency: every operation holds one mutex end to end, so a client_id
// never has two operations in flight. That is not just tidiness — it is
// the contract of known flag #1 (apply-time dedup drops out-of-order seq).
class RaftGroupClient {
public:
    enum class Status {
        kOk,
        kNotFound,     // Get only: the key does not exist
        kUnavailable,  // no leader reachable / no commit within overall timeout
        kError,        // non-retriable: bad arguments or server said ERROR
        kWrongShard,   // the node answered "WRONG_SHARD <v>": it does not own this key
    };

    struct Result {
        Status status = Status::kError;
        std::string value;   // Get: the value
        std::string detail;  // human-readable reason for non-kOk
        // kWrongShard only: the shard-map version of the node that rejected
        // the key (0 if the node sent none). The router compares it with
        // its cached version to decide whether a refetch can help.
        std::uint64_t map_version = 0;
    };

    RaftGroupClient(std::vector<Endpoint> endpoints, RaftGroupClientOptions options = {});

    Result Put(const std::string& key, const std::string& value);
    Result Delete(const std::string& key);
    // Reads from the node that currently CLAIMS leadership (a LEADER probe
    // first). Not linearizable — see known flag #2.
    Result Get(const std::string& key);

    std::uint64_t client_id() const { return client_id_; }

private:
    Result SendWrite(const std::string& line);

    std::mutex mu_;
    std::vector<Endpoint> endpoints_;
    RaftGroupClientOptions options_;
    std::uint64_t client_id_;
    std::uint64_t next_seq_ = 1;
    std::optional<Endpoint> leader_;  // last node that confirmed leadership
};

}  // namespace sharding