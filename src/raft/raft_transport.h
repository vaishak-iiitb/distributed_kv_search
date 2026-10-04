#pragma once

#include <atomic>
#include <cstdint>
#include <functional>
#include <map>
#include <mutex>
#include <string>
#include <thread>
#include <vector>

#include "net/thread_pool.h"
#include "raft/rpc.h"

namespace raft {

struct PeerAddress {
    std::string host;
    int port = 0;
};

// Real TCP transport for Raft's inter-node RPCs, using Step 1's framed
// binary wire format. Separate from TCPServer (client-facing KV text
// protocol, different port, different purpose) — see design notes §2.1.
//
// Delivers (NodeId, Message) pairs to a callback. Does NOT call into
// PersistentRaftNode and is not responsible for making that safe under
// concurrency — that wiring is Step 8b's job (see design notes §3.4's
// closing note and §4's last row).
class RaftTransport {
public:
    using MessageHandler = std::function<void(NodeId from, const Message& message)>;

    // `peers` must list every OTHER node in the cluster (not self).
    RaftTransport(NodeId self_id, int listen_port, std::map<NodeId, PeerAddress> peers,
                 MessageHandler on_message, std::size_t thread_pool_size = 4);
    ~RaftTransport();

    RaftTransport(const RaftTransport&) = delete;
    RaftTransport& operator=(const RaftTransport&) = delete;

    // Binds, listens, and starts accepting connections on a background
    // thread. False on any socket setup failure.
    bool Start();

    // Stops accepting new connections, unblocks and closes every
    // connection (inbound and outbound), and joins background threads.
    // Safe to call more than once. The destructor calls this too.
    void Stop();

    // Best-effort, asynchronous send: encodes `message` and dispatches
    // the actual I/O to a pool thread, returning immediately. `to` must
    // be a real peer id from the `peers` map, never kBroadcast — see
    // design notes §3.2 for why that expansion happens one layer up.
    // Silently drops the message if the peer is unreachable (design
    // notes §2.4).
    void Send(NodeId to, const Message& message);

private:
    void AcceptLoop();
    void HandleInboundConnection(int fd);
    // Caller must already hold outbound_mutex_ — see design notes §3.5.
    bool ConnectLocked(NodeId to);

    NodeId self_id_;
    int listen_port_;
    std::map<NodeId, PeerAddress> peers_;
    MessageHandler on_message_;

    int listen_fd_ = -1;
    std::atomic<bool> running_{false};
    std::thread accept_thread_;

    std::mutex outbound_mutex_;
    std::map<NodeId, int> outbound_fds_;  // peer -> connected socket, lazily opened

    std::mutex inbound_mutex_;
    std::vector<int> inbound_fds_;  // every currently-accepted connection — see design notes §3.4

    // Declared LAST so it is destroyed FIRST (member destruction runs in
    // reverse declaration order). This guarantees every queued/in-flight
    // Send() task has fully finished — including releasing its lock on
    // outbound_mutex_ — before outbound_mutex_ (or anything else above)
    // is destroyed. See design notes §3.5; declaring this any earlier
    // risks a worker thread locking an already-destroyed mutex during
    // teardown.
    ThreadPool pool_;
};

}  // namespace raft