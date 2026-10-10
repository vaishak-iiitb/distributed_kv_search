#pragma once

#include <atomic>
#include <map>
#include <mutex>
#include <string>
#include <thread>
#include <vector>
#include <atomic>
#include <functional>
#include <map>
#include <memory>

#include "net/thread_pool.h"
#include "raft/raft_node.h"

namespace raft {

// The client-facing server (design notes §1/§3.1) — a new, separate
// server from TCPServer, on its own port, speaking the line protocol
// specified in design notes §2.1. Wraps a RaftNode: every write goes
// through propose-and-wait-for-commit (Step 9c part 1), every rejected
// write carries a redirect hint if one is known (Step 9b), and every
// write is deduplicated by (client_id, sequence_num) at apply time
// (Step 9a).
class ClientServer {
public:
        // `client_peer_addresses` maps every OTHER node's id to its OWN
    // client-facing address — NOT its Raft-to-Raft address. Those are
    // two different port spaces; RaftNode's own peer map can't be
    // reused for this.
    ClientServer(int port, RaftNode* node, std::map<NodeId, PeerAddress> client_peer_addresses,
                Duration propose_timeout = Duration(2000), std::size_t thread_pool_size = 4);
    ~ClientServer();

    ClientServer(const ClientServer&) = delete;
    ClientServer& operator=(const ClientServer&) = delete;

    bool Start();
    void Stop();

        // Optional per-key admission check, consulted by GET / PUT / DELETE
    // BEFORE anything is proposed or read. Returns "" to admit the key, or
    // a complete response line (including its trailing "\n") to send back
    // instead. With no guard installed (the default) every key is admitted,
    // so Phase 1/2 groups and the metadata group behave exactly as before.
    // Safe to call at any time, including while serving.
    using KeyGuard = std::function<std::string(const std::string& key)>;
    void SetKeyGuard(KeyGuard guard);

private:
    void AcceptLoop();
    void HandleClient(int fd);
    std::string HandleLine(const std::string& line);
    std::string FormatProposeOutcome(const RaftNode::ProposeWaitResult& result);
    std::string RejectIfNotOwned(const std::string& key);
    // Shared by write rejections and the LEADER probe: "NOT_LEADER host:port"
    // when the leader is known and has a client-facing address, else a bare
    // "NOT_LEADER". Uses client_peer_addresses_, NOT RaftNode's Raft-to-Raft map.
    std::string NotLeaderResponse();

    int port_;
    RaftNode* node_;
    std::map<NodeId, PeerAddress> client_peer_addresses_;
    Duration propose_timeout_;

    int listen_fd_ = -1;
    std::atomic<bool> running_{false};
    std::thread accept_thread_;

    std::mutex inbound_mutex_;
    std::vector<int> inbound_fds_;

    std::mutex guard_mutex_;
    std::shared_ptr<const KeyGuard> key_guard_;  // null = admit everything

    // Declared last so it is destroyed first — see design notes §3.2
    // (reusing the exact lesson from Step 8a's RaftTransport).
    ThreadPool pool_;
};

}  // namespace raft