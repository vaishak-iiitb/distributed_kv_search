#pragma once

#include <atomic>
#include <map>
#include <mutex>
#include <string>
#include <thread>
#include <vector>

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

private:
    void AcceptLoop();
    void HandleClient(int fd);
    std::string HandleLine(const std::string& line);
    std::string FormatProposeOutcome(const RaftNode::ProposeWaitResult& result);

    int port_;
    RaftNode* node_;
    std::map<NodeId, PeerAddress> client_peer_addresses_;
    Duration propose_timeout_;

    int listen_fd_ = -1;
    std::atomic<bool> running_{false};
    std::thread accept_thread_;

    std::mutex inbound_mutex_;
    std::vector<int> inbound_fds_;

    // Declared last so it is destroyed first — see design notes §3.2
    // (reusing the exact lesson from Step 8a's RaftTransport).
    ThreadPool pool_;
};

}  // namespace raft