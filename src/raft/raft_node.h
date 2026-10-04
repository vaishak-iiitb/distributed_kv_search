#pragma once

#include <atomic>
#include <cstdint>
#include <map>
#include <mutex>
#include <string>
#include <thread>

#include "raft/election_timer.h"
#include "raft/persistent_node.h"
#include "raft/raft_transport.h"

namespace raft {

struct RaftNodeConfig {
    NodeId self_id = 0;
    int listen_port = 0;
    std::map<NodeId, PeerAddress> peers;  // every OTHER node in the cluster
    std::string state_path;
    std::string log_path;
    std::string kv_data_dir;

    Duration election_timeout_min = Duration(150);
    Duration election_timeout_max = Duration(300);
    Duration heartbeat_interval = Duration(50);
    std::uint64_t election_timer_seed = 0;
};

// The production driving loop. Owns a PersistentRaftNode, a
// RaftTransport, and an ElectionTimer, and ties them together with a
// background thread that fires ElectionTimeout on real timeout and
// sends periodic leader heartbeats — this project's first real use of
// ElectionTimer against actual elapsed time (Step 3a).
//
// Every access to the underlying PersistentRaftNode goes through
// node_mutex_ — this is where the "no locking" gap flagged since Step
// 2a's design notes (§4) is closed, in one place, rather than inside
// state_machine.h itself.
class RaftNode {
public:
    explicit RaftNode(RaftNodeConfig config);
    ~RaftNode();

    RaftNode(const RaftNode&) = delete;
    RaftNode& operator=(const RaftNode&) = delete;

    // Starts the transport and the background driving thread. False if
    // the transport fails to bind/listen, or if the timing config fails
    // validate_timing_config (Step 3a) — see design notes §3.4.
    bool Start();
    void Stop();

    // Thread-safe. NOT linearizable yet — see Step 7b design notes §2.3,
    // still true here; real client redirect and read guarantees are
    // Step 9.
    PersistentRaftNode::DurableProposeResult ProposePut(const std::string& key,
                                                        const std::string& value);
    PersistentRaftNode::DurableProposeResult ProposeDelete(const std::string& key);
    bool Get(const std::string& key, std::string* out_value);

    Role CurrentRole();

private:
    void OnMessage(NodeId from, const Message& message);
    void DrivingLoop();
    void DispatchAndSend(const HandleResult& result);
    void ResetElectionTimerIfWarranted(const HandleResult& result);  // caller holds node_mutex_
    AppendEntriesRequest BuildHeartbeatFor(NodeId peer);             // caller holds node_mutex_

    RaftNodeConfig config_;
    std::size_t cluster_size_;

    std::mutex node_mutex_;
    PersistentRaftNode node_;
    ElectionTimer election_timer_;

    RaftTransport transport_;

    std::atomic<bool> running_{false};
    std::thread driving_thread_;
    TimePoint last_heartbeat_sent_;
};

}  // namespace raft