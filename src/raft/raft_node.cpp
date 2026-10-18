#include "raft/raft_node.h"

#include <chrono>
#include <thread>
#include <type_traits>
#include <variant>

namespace raft {

RaftNode::RaftNode(RaftNodeConfig config)
    : config_(config),
      cluster_size_(config.peers.size() + 1),
      node_(config.self_id, config.state_path, config.log_path, config.kv_data_dir),
      election_timer_(config.election_timeout_min, config.election_timeout_max,
                      config.election_timer_seed),
      transport_(config.self_id, config.listen_port, config.peers,
                [this](NodeId from, const Message& msg) { OnMessage(from, msg); }) {}

RaftNode::~RaftNode() { Stop(); }

bool RaftNode::Start() {
    if (!validate_timing_config(config_.heartbeat_interval, config_.election_timeout_min,
                                config_.election_timeout_max)) {
        return false;
    }
    if (!transport_.Start()) return false;

    {
        std::lock_guard<std::mutex> lock(node_mutex_);
        election_timer_.reset(Clock::now());
    }
    last_heartbeat_sent_ = Clock::now();

    running_ = true;
    driving_thread_ = std::thread(&RaftNode::DrivingLoop, this);
    return true;
}

void RaftNode::Stop() {
    if (!running_.exchange(false)) return;
    if (driving_thread_.joinable()) driving_thread_.join();
    transport_.Stop();
    // Wake any still-waiting ProposePutAndWait/ProposeDeleteAndWait
    // callers so they don't block past Stop() returning — their
    // predicate will see role/term in whatever final state they're in
    // and resolve to lost_leadership rather than hanging.
    state_changed_cv_.notify_all();
}

void RaftNode::DispatchAndSend(const HandleResult& result) {
    for (const auto& out : result.messages) {
        if (out.to == kBroadcast) {
            for (const auto& [peer_id, addr] : config_.peers) {
                (void)addr;
                transport_.Send(peer_id, out.message);
            }
        } else {
            transport_.Send(out.to, out.message);
        }
    }
}

void RaftNode::ResetElectionTimerIfWarranted(const HandleResult& result) {
    for (const auto& out : result.messages) {
        if (const auto* vr = std::get_if<RequestVoteResponse>(&out.message)) {
            if (vr->vote_granted) {
                election_timer_.reset(Clock::now());
                return;
            }
        }
        if (const auto* ar = std::get_if<AppendEntriesResponse>(&out.message)) {
            if (ar->success) {
                election_timer_.reset(Clock::now());
                return;
            }
        }
    }
}

void RaftNode::UpdateKnownLeader(const Event& event, const HandleResult& result) {
    const Role role = node_.state().role;

    if (role == Role::kCandidate) {
        known_leader_.reset();
        return;
    }
    if (role == Role::kLeader) {
        known_leader_ = config_.self_id;
        return;
    }

    if (const auto* received = std::get_if<Received<AppendEntriesRequest>>(&event)) {
        for (const auto& out : result.messages) {
            if (const auto* resp = std::get_if<AppendEntriesResponse>(&out.message)) {
                if (resp->success) {
                    known_leader_ = received->message.leader_id;
                }
            }
        }
    }
}

AppendEntriesRequest RaftNode::BuildHeartbeatFor(NodeId peer) {
    const RaftState& state = node_.state();

    AppendEntriesRequest req;
    req.term = state.current_term;
    req.leader_id = state.self_id;
    req.leader_commit = state.commit_index;

    const auto it = state.next_index.find(peer);
    if (it != state.next_index.end()) {
        const LogIndex next = it->second;
        req.prev_log_index = next - 1;
        req.prev_log_term = state.log.term_at(next - 1);
        if (next <= state.log.last_index()) {
            req.entries = {*state.log.at(next)};
        }
    } else {
        req.prev_log_index = state.log.last_index();
        req.prev_log_term = state.log.last_term();
    }
    return req;
}

void RaftNode::OnMessage(NodeId from, const Message& message) {
    const Event event = std::visit(
        [](const auto& msg) -> Event {
            using T = std::decay_t<decltype(msg)>;
            return Received<T>{msg};
        },
        message);

    HandleResult result;
    HandleResult noop_to_send;
    bool ok = false;
    {
        std::lock_guard<std::mutex> lock(node_mutex_);
        const auto durable = node_.Handle(from, event, cluster_size_);
        if (!durable.persisted) {
            return;
        }
        ResetElectionTimerIfWarranted(durable.result);
        UpdateKnownLeader(event, durable.result);
        // Flag #2 fix: a new leader immediately appends an entry of its own
        // term so that older entries can become committed and applied.
        if (durable.result.became_leader) noop_to_send = AppendLeaderNoopLocked();
        result = durable.result;
        ok = true;
        state_changed_cv_.notify_all();  // Step 9c — see design notes §3.2
    }
    if (ok) {
        DispatchAndSend(result);
        DispatchAndSend(noop_to_send);
    }
}

HandleResult RaftNode::AppendLeaderNoopLocked() {
    have_barrier_ = false;
    const auto noop = node_.ProposeNoop();
    if (!noop.persisted || !noop.result.accepted) return HandleResult{};
    have_barrier_ = true;
    barrier_term_ = node_.state().current_term;
    barrier_index_ = noop.result.index;
    return noop.result.to_send;
}

bool RaftNode::ReadBarrierPassedLocked() const {
    const RaftState& state = node_.state();
    return have_barrier_ && state.role == Role::kLeader && state.current_term == barrier_term_ &&
           state.commit_index >= barrier_index_ && state.last_applied >= barrier_index_;
}

void RaftNode::DrivingLoop() {
    while (running_) {
        std::this_thread::sleep_for(std::chrono::milliseconds(10));
        if (!running_) break;

        const TimePoint now = Clock::now();

        std::vector<std::pair<NodeId, AppendEntriesRequest>> heartbeats;
        bool maybe_election_timeout = false;

        {
            std::lock_guard<std::mutex> lock(node_mutex_);
            if (node_.state().role == Role::kLeader) {
                if (now - last_heartbeat_sent_ >= config_.heartbeat_interval) {
                    for (const auto& [peer_id, addr] : config_.peers) {
                        (void)addr;
                        heartbeats.emplace_back(peer_id, BuildHeartbeatFor(peer_id));
                    }
                    last_heartbeat_sent_ = now;
                }
            } else if (election_timer_.has_expired(now)) {
                maybe_election_timeout = true;
            }
        }

        for (const auto& [peer_id, req] : heartbeats) {
            transport_.Send(peer_id, req);
        }

        if (maybe_election_timeout) {
            HandleResult result;
            bool ok = false;
            {
                std::lock_guard<std::mutex> lock(node_mutex_);
                if (node_.state().role != Role::kLeader && election_timer_.has_expired(now)) {
                    const Event event = ElectionTimeout{};
                    const auto durable = node_.Handle(config_.self_id, event, cluster_size_);
                    if (durable.persisted) {
                        election_timer_.reset(now);
                        UpdateKnownLeader(event, durable.result);
                        result = durable.result;
                        ok = true;
                        state_changed_cv_.notify_all();  // Step 9c
                    }
                }
            }
            if (ok) DispatchAndSend(result);
        }
    }
}

PersistentRaftNode::DurableProposeResult RaftNode::ProposePut(ClientId client_id,
                                                               SequenceNum sequence_num,
                                                               const std::string& key,
                                                               const std::string& value) {
    PersistentRaftNode::DurableProposeResult durable;
    HandleResult to_send;
    bool should_send = false;
    {
        std::lock_guard<std::mutex> lock(node_mutex_);
        durable = node_.ProposePut(client_id, sequence_num, key, value);
        if (durable.persisted && durable.result.accepted) {
            to_send = durable.result.to_send;
            should_send = true;
        }
        state_changed_cv_.notify_all();
    }
    if (should_send) DispatchAndSend(to_send);
    return durable;
}

PersistentRaftNode::DurableProposeResult RaftNode::ProposeDelete(ClientId client_id,
                                                                  SequenceNum sequence_num,
                                                                  const std::string& key) {
    PersistentRaftNode::DurableProposeResult durable;
    HandleResult to_send;
    bool should_send = false;
    {
        std::lock_guard<std::mutex> lock(node_mutex_);
        durable = node_.ProposeDelete(client_id, sequence_num, key);
        if (durable.persisted && durable.result.accepted) {
            to_send = durable.result.to_send;
            should_send = true;
        }
        state_changed_cv_.notify_all();
    }
    if (should_send) DispatchAndSend(to_send);
    return durable;
}

// Step 9c: shared wait logic — see design notes §3.1/§3.3.
RaftNode::ProposeWaitResult RaftNode::WaitForCommit(
    Term proposed_term, LogIndex proposed_index, Duration timeout,
    PersistentRaftNode::DurableProposeResult durable) {
    ProposeWaitResult result;
    result.durable = std::move(durable);

    std::unique_lock<std::mutex> lock(node_mutex_);
    const bool predicate_met = state_changed_cv_.wait_for(lock, timeout, [&] {
        return (node_.state().current_term == proposed_term &&
               node_.state().commit_index >= proposed_index) ||
               node_.state().role != Role::kLeader ||
               node_.state().current_term != proposed_term;
    });

    if (node_.state().current_term == proposed_term &&
        node_.state().commit_index >= proposed_index) {
        result.committed = true;
    } else if (!predicate_met) {
        result.timed_out = true;
    } else {
        result.lost_leadership = true;
    }
    return result;
}

RaftNode::ProposeWaitResult RaftNode::ProposePutAndWait(ClientId client_id,
                                                         SequenceNum sequence_num,
                                                         const std::string& key,
                                                         const std::string& value,
                                                         Duration timeout) {
    PersistentRaftNode::DurableProposeResult durable;
    Term proposed_term = 0;
    LogIndex proposed_index = 0;
    {
        std::lock_guard<std::mutex> lock(node_mutex_);
        durable = node_.ProposePut(client_id, sequence_num, key, value);
        if (!durable.persisted || !durable.result.accepted) {
            ProposeWaitResult result;
            result.durable = std::move(durable);
            return result;  // nothing to wait for — not leader, or persist failed
        }
        proposed_term = node_.state().current_term;
        proposed_index = durable.result.index;
        state_changed_cv_.notify_all();
    }
    DispatchAndSend(durable.result.to_send);

    return WaitForCommit(proposed_term, proposed_index, timeout, std::move(durable));
}

RaftNode::ProposeWaitResult RaftNode::ProposeDeleteAndWait(ClientId client_id,
                                                            SequenceNum sequence_num,
                                                            const std::string& key,
                                                            Duration timeout) {
    PersistentRaftNode::DurableProposeResult durable;
    Term proposed_term = 0;
    LogIndex proposed_index = 0;
    {
        std::lock_guard<std::mutex> lock(node_mutex_);
        durable = node_.ProposeDelete(client_id, sequence_num, key);
        if (!durable.persisted || !durable.result.accepted) {
            ProposeWaitResult result;
            result.durable = std::move(durable);
            return result;
        }
        proposed_term = node_.state().current_term;
        proposed_index = durable.result.index;
        state_changed_cv_.notify_all();
    }
    DispatchAndSend(durable.result.to_send);

    return WaitForCommit(proposed_term, proposed_index, timeout, std::move(durable));
}

bool RaftNode::Get(const std::string& key, std::string* out_value) {
    std::lock_guard<std::mutex> lock(node_mutex_);
    return node_.Get(key, out_value);
}

RaftNode::ReadResult RaftNode::GetFromLeader(const std::string& key, Duration timeout) {
    ReadResult out;
    std::unique_lock<std::mutex> lock(node_mutex_);
    state_changed_cv_.wait_for(lock, timeout, [&] {
        return !running_.load() || node_.state().role != Role::kLeader || ReadBarrierPassedLocked();
    });
    if (!running_.load() || node_.state().role != Role::kLeader) {
        out.status = ReadStatus::kNotLeader;
        return out;
    }
    if (!ReadBarrierPassedLocked()) {
        out.status = ReadStatus::kNotReady;
        return out;
    }
    std::string value;
    if (node_.Get(key, &value)) {
        out.status = ReadStatus::kFound;
        out.value = std::move(value);
    } else {
        out.status = ReadStatus::kNotFound;
    }
    return out;
}

Role RaftNode::CurrentRole() {
    std::lock_guard<std::mutex> lock(node_mutex_);
    return node_.state().role;
}

RaftNode::LeaderHint RaftNode::KnownLeaderHint() {
    std::lock_guard<std::mutex> lock(node_mutex_);
    LeaderHint hint;
    hint.leader_id = known_leader_;
    if (known_leader_.has_value() && *known_leader_ != config_.self_id) {
        const auto it = config_.peers.find(*known_leader_);
        if (it != config_.peers.end()) hint.address = it->second;
    }
    return hint;
}

}  // namespace raft