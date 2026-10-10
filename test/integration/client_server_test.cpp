#include "raft/client_server.h"
#include "raft/raft_node.h"

#include <arpa/inet.h>
#include <netinet/in.h>
#include <sys/socket.h>
#include <unistd.h>

#include <chrono>
#include <cstdio>
#include <functional>
#include <memory>
#include <optional>
#include <string>
#include <thread>
#include <vector>

namespace {

int g_failures = 0;

#define CHECK(cond)                                                          \
    do {                                                                     \
        if (!(cond)) {                                                       \
            std::fprintf(stderr, "%s:%d: CHECK failed: %s\n", __FILE__,      \
                         __LINE__, #cond);                                   \
            ++g_failures;                                                    \
        }                                                                    \
    } while (0)

void remove_if_exists(const std::string& path) { std::remove(path.c_str()); }

bool WaitUntil(const std::function<bool()>& predicate, std::chrono::milliseconds timeout,
              std::chrono::milliseconds poll = std::chrono::milliseconds(20)) {
    const auto deadline = std::chrono::steady_clock::now() + timeout;
    while (std::chrono::steady_clock::now() < deadline) {
        if (predicate()) return true;
        std::this_thread::sleep_for(poll);
    }
    return predicate();
}

// A minimal, synchronous, one-shot raw-socket client: connect, send one
// line, read one line back, disconnect. Good enough to exercise the
// protocol; not meant to be a reusable client library.
std::optional<std::string> SendLine(int port, const std::string& request_line) {
    const int fd = ::socket(AF_INET, SOCK_STREAM, 0);
    if (fd < 0) return std::nullopt;

    sockaddr_in addr{};
    addr.sin_family = AF_INET;
    addr.sin_port = htons(static_cast<uint16_t>(port));
    ::inet_pton(AF_INET, "127.0.0.1", &addr.sin_addr);

    if (::connect(fd, reinterpret_cast<sockaddr*>(&addr), sizeof(addr)) < 0) {
        ::close(fd);
        return std::nullopt;
    }

    const std::string line = request_line + "\n";
    if (::write(fd, line.data(), line.size()) < 0) {
        ::close(fd);
        return std::nullopt;
    }

    std::string response;
    char buf[4096];
    while (response.find('\n') == std::string::npos) {
        const ssize_t n = ::read(fd, buf, sizeof(buf));
        if (n <= 0) {
            ::close(fd);
            return response.empty() ? std::nullopt : std::make_optional(response);
        }
        response.append(buf, static_cast<std::size_t>(n));
    }
    ::close(fd);

    if (!response.empty() && response.back() == '\n') response.pop_back();
    return response;
}

raft::RaftNodeConfig MakeConfig(raft::NodeId id, int base_port,
                                const std::vector<raft::NodeId>& all_ids,
                                const std::string& prefix) {
    raft::RaftNodeConfig cfg;
    cfg.self_id = id;
    cfg.listen_port = base_port + static_cast<int>(id);
    for (raft::NodeId peer : all_ids) {
        if (peer == id) continue;
        cfg.peers[peer] = raft::PeerAddress{"127.0.0.1", base_port + static_cast<int>(peer)};
    }
    cfg.state_path = prefix + "_n" + std::to_string(id) + ".state";
    cfg.log_path = prefix + "_n" + std::to_string(id) + ".log";
    cfg.kv_data_dir = prefix + "_n" + std::to_string(id) + ".kv";
    cfg.election_timeout_min = raft::Duration(100);
    cfg.election_timeout_max = raft::Duration(200);
    cfg.heartbeat_interval = raft::Duration(30);
    cfg.election_timer_seed = 2000 + id;
    return cfg;
}

void CleanNodeFiles(const std::string& prefix, const std::vector<raft::NodeId>& ids) {
    for (raft::NodeId id : ids) {
        remove_if_exists(prefix + "_n" + std::to_string(id) + ".state");
        remove_if_exists(prefix + "_n" + std::to_string(id) + ".log");
    }
}

std::optional<raft::NodeId> FindLeader(std::vector<std::unique_ptr<raft::RaftNode>>& nodes,
                                       const std::vector<raft::NodeId>& ids) {
    for (std::size_t i = 0; i < nodes.size(); ++i) {
        if (nodes[i]->CurrentRole() == raft::Role::kLeader) return ids[i];
    }
    return std::nullopt;
}

// ---------------------------------------------------------------------------

void test_put_get_and_redirect_over_real_sockets() {
    const std::vector<raft::NodeId> ids = {0, 1, 2};
    const std::string prefix = "client_server_test";
    CleanNodeFiles(prefix, ids);
    const int raft_base_port = 18600;
    const int client_base_port = 18700;

    std::vector<std::unique_ptr<raft::RaftNode>> nodes;
    std::vector<std::unique_ptr<raft::ClientServer>> servers;
    for (raft::NodeId id : ids) {
        nodes.push_back(
            std::make_unique<raft::RaftNode>(MakeConfig(id, raft_base_port, ids, prefix)));
        CHECK(nodes.back()->Start());

        // Every OTHER node's id mapped to ITS client-facing address —
        // separate from RaftNode's own Raft-to-Raft peer map.
        std::map<raft::NodeId, raft::PeerAddress> client_peer_addresses;
        for (raft::NodeId peer : ids) {
            if (peer == id) continue;
            client_peer_addresses[peer] =
                raft::PeerAddress{"127.0.0.1", client_base_port + static_cast<int>(peer)};
        }

        servers.push_back(std::make_unique<raft::ClientServer>(
            client_base_port + static_cast<int>(id), nodes.back().get(),
            std::move(client_peer_addresses)));
        CHECK(servers.back()->Start());
    }

    CHECK(WaitUntil([&] { return FindLeader(nodes, ids).has_value(); },
                    std::chrono::milliseconds(3000)));
    const auto leader_id = FindLeader(nodes, ids);
    CHECK(leader_id.has_value());
    if (!leader_id) return;

    // A write to a FOLLOWER must be redirected, not silently dropped —
    // and must redirect to the real leader's actual client-facing port
    // once that follower has learned who leads (Step 9b, exercised here
    // through the real wire protocol instead of KnownLeaderHint() directly).
    raft::NodeId follower_id = (*leader_id == 0) ? 1 : 0;
    CHECK(WaitUntil(
        [&] {
            const auto resp = SendLine(client_base_port + static_cast<int>(follower_id),
                                       "PUT 1 1 x 1");
            return resp.has_value() && resp->rfind("NOT_LEADER ", 0) == 0;
        },
        std::chrono::milliseconds(2000)));

    const auto redirect_resp =
        SendLine(client_base_port + static_cast<int>(follower_id), "PUT 1 1 x 1");
    CHECK(redirect_resp.has_value());
    CHECK(redirect_resp->rfind("NOT_LEADER 127.0.0.1:", 0) == 0);
    const std::string expected_port =
        std::to_string(client_base_port + static_cast<int>(*leader_id));
    CHECK(redirect_resp->find(expected_port) != std::string::npos);
        // LEADER probe: the leader confirms itself; a follower redirects to it.
    const auto leader_probe =
        SendLine(client_base_port + static_cast<int>(*leader_id), "LEADER");
    CHECK(leader_probe.has_value() && *leader_probe == "LEADER");
    const auto follower_probe =
        SendLine(client_base_port + static_cast<int>(follower_id), "LEADER");
    CHECK(follower_probe.has_value() &&
          *follower_probe == "NOT_LEADER 127.0.0.1:" + expected_port);

    // A write to the REAL leader must succeed.
    const auto put_resp =
        SendLine(client_base_port + static_cast<int>(*leader_id), "PUT 1 1 x 1");
    CHECK(put_resp.has_value() && *put_resp == "OK");

    // Readable immediately on the leader.
    const auto get_resp = SendLine(client_base_port + static_cast<int>(*leader_id), "GET x");
    CHECK(get_resp.has_value() && *get_resp == "VALUE 1");

    // A value containing a space round-trips correctly (design notes §2.1).
    const auto put_spaced =
        SendLine(client_base_port + static_cast<int>(*leader_id), "PUT 1 2 greeting hello world");
    CHECK(put_spaced.has_value() && *put_spaced == "OK");
    const auto get_spaced = SendLine(client_base_port + static_cast<int>(*leader_id), "GET greeting");
    CHECK(get_spaced.has_value() && *get_spaced == "VALUE hello world");

    // A missing key.
    const auto get_missing =
        SendLine(client_base_port + static_cast<int>(*leader_id), "GET does_not_exist");
    CHECK(get_missing.has_value() && *get_missing == "NOT_FOUND");

    // A retried write (same client_id/sequence_num) must still report OK
    // (it's a safe no-op per Step 9a) and must not change the value.
    const auto retry_resp =
        SendLine(client_base_port + static_cast<int>(*leader_id), "PUT 1 1 x 999");
    CHECK(retry_resp.has_value() && *retry_resp == "OK");
    const auto get_after_retry = SendLine(client_base_port + static_cast<int>(*leader_id), "GET x");
    CHECK(get_after_retry.has_value() && *get_after_retry == "VALUE 1");  // unchanged

    // DELETE.
    const auto delete_resp =
        SendLine(client_base_port + static_cast<int>(*leader_id), "DELETE 1 3 x");
    CHECK(delete_resp.has_value() && *delete_resp == "OK");
    const auto get_after_delete = SendLine(client_base_port + static_cast<int>(*leader_id), "GET x");
    CHECK(get_after_delete.has_value() && *get_after_delete == "NOT_FOUND");

    // Malformed request.
    const auto malformed_resp =
        SendLine(client_base_port + static_cast<int>(*leader_id), "PUT not enough fields");
    CHECK(malformed_resp.has_value() && malformed_resp->rfind("ERROR", 0) == 0);

    for (auto& server : servers) server->Stop();
    for (auto& node : nodes) node->Stop();
    CleanNodeFiles(prefix, ids);
}

}  // namespace

int main() {
    test_put_get_and_redirect_over_real_sockets();

    if (g_failures == 0) {
        std::printf("client_server_test: all checks passed\n");
        return 0;
    }
    std::fprintf(stderr, "client_server_test: %d check(s) failed\n", g_failures);
    return 1;
}