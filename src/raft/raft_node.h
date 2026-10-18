#pragma once

#include <atomic>
#include <condition_variable>
#include <cstdint>
#include <map>
#include <mutex>
#include <optional>
#include <string>
#include <thread>

#include "raft/election_timer.h"
#include "raft/persistent_node.h"
#include "raft/raft_transport.h"

namespace raft {

struct RaftNodeConfig {
    NodeId self_id = 0;
    int listen_port = 0;
    std::map<NodeId, PeerAddress> peers;
    std::string state_path;
    std::string log_path;
    std::string kv_data_dir;

    Duration election_timeout_min = Duration(150);
    Duration election_timeout_max = Duration(300);
    Duration heartbeat_interval = Duration(50);
    std::uint64_t election_timer_seed = 0;
};

class RaftNode {
public:
    explicit RaftNode(RaftNodeConfig config);
    ~RaftNode();

    RaftNode(const RaftNode&) = delete;
    RaftNode& operator=(const RaftNode&) = delete;

    bool Start();
    void Stop();

    // Non-blocking: returns once the entry is proposed and persisted
    // locally, NOT once committed. Kept for callers that manage their
    // own waiting (e.g. tests) — see *_AndWait below for the
    // commit-aware versions a real client should use.
    PersistentRaftNode::DurableProposeResult ProposePut(ClientId client_id,
                                                        SequenceNum sequence_num,
                                                        const std::string& key,
                                                        const std::string& value);
    PersistentRaftNode::DurableProposeResult ProposeDelete(ClientId client_id,
                                                           SequenceNum sequence_num,
                                                           const std::string& key);

    // Step 9c: blocks until the proposed entry commits, the outcome
    // becomes ambiguous (leadership/term changed), or `timeout` elapses
    // — see design notes §2.2 for what each outcome means and why only
    // `committed` is a confident success.
    struct ProposeWaitResult {
        PersistentRaftNode::DurableProposeResult durable;
        bool committed = false;
        bool lost_leadership = false;
        bool timed_out = false;
    };
    ProposeWaitResult ProposePutAndWait(ClientId client_id, SequenceNum sequence_num,
                                        const std::string& key, const std::string& value,
                                        Duration timeout);
    ProposeWaitResult ProposeDeleteAndWait(ClientId client_id, SequenceNum sequence_num,
                                           const std::string& key, Duration timeout);

    bool Get(const std::string& key, std::string* out_value);

    // Flag #2 fix. A read the LEADER may serve: it waits (up to `timeout`)
    // until this leader has committed and applied the no-op it appended when
    // it was elected, which guarantees every entry committed in earlier terms
    // is already in the state machine. kNotLeader means this node is not (or
    // is no longer) the leader; kNotReady means the barrier did not clear in
    // time (e.g. no majority reachable).
    // NOTE: this closes the "new leader serves a stale read" window. It does
    // NOT stop a deposed leader that has not yet noticed it was replaced from
    // answering (that needs a ReadIndex-style leadership check; see docs).
    enum class ReadStatus { kFound, kNotFound, kNotLeader, kNotReady };
    struct ReadResult {
        ReadStatus status = ReadStatus::kNotLeader;
        std::string value;
    };
    ReadResult GetFromLeader(const std::string& key, Duration timeout);

    Role CurrentRole();

    struct LeaderHint {
        std::optional<NodeId> leader_id;
        std::optional<PeerAddress> address;
    };
    LeaderHint KnownLeaderHint();

private:
    void OnMessage(NodeId from, const Message& message);
    void DrivingLoop();
    void DispatchAndSend(const HandleResult& result);
    void ResetElectionTimerIfWarranted(const HandleResult& result);
    void UpdateKnownLeader(const Event& event, const HandleResult& result);
    AppendEntriesRequest BuildHeartbeatFor(NodeId peer);

    // Flag #2 fix. Caller must hold node_mutex_.
    // Appends the new-term no-op and records it as the read barrier; returns
    // the messages to broadcast (empty if the append failed).
    HandleResult AppendLeaderNoopLocked();
    bool ReadBarrierPassedLocked() const;

    // Step 9c: shared by both *_AndWait methods — see design notes §3.1.
    // Caller must NOT hold node_mutex_ when calling this.
    ProposeWaitResult WaitForCommit(Term proposed_term, LogIndex proposed_index,
                                    Duration timeout,
                                    PersistentRaftNode::DurableProposeResult durable);

    RaftNodeConfig config_;
    std::size_t cluster_size_;

    std::mutex node_mutex_;
    PersistentRaftNode node_;
    ElectionTimer election_timer_;
    std::optional<NodeId> known_leader_;

    // Flag #2 fix (guarded by node_mutex_): the no-op this node appended when
    // it last became leader. Reads are allowed once it is committed+applied.
    bool have_barrier_ = false;
    Term barrier_term_ = 0;
    LogIndex barrier_index_ = 0;

    // Step 9c: notified after every Handle() call that actually ran
    // (persisted == true) — see design notes §3.2 for why unconditional
    // notification is the right call here.
    std::condition_variable state_changed_cv_;

    RaftTransport transport_;

    std::atomic<bool> running_{false};
    std::thread driving_thread_;
    TimePoint last_heartbeat_sent_;
};

}  // namespace raft