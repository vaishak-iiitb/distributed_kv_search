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
        return false;  // design notes §3.4 — refuse rather than start unsafe
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
    // Only now does the transport shut down — see design notes §3.5.
    transport_.Stop();
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
    // Caller holds node_mutex_. See design notes §2.2.
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

AppendEntriesRequest RaftNode::BuildHeartbeatFor(NodeId peer) {
    // Caller holds node_mutex_. See design notes §2.1 for why this
    // cannot simply always use the leader's raw log tip.
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
        // Untracked peer: same tip-based, empty-entries shape as the
        // one-shot announcement (Step 2b) and Step 6's first-contact
        // trigger.
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
    bool ok = false;
    {
        std::lock_guard<std::mutex> lock(node_mutex_);
        const auto durable = node_.Handle(from, event, cluster_size_);
        if (!durable.persisted) {
            return;  // design notes §3.6 — passive, named limitation
        }
        ResetElectionTimerIfWarranted(durable.result);
        result = durable.result;
        ok = true;
    }
    if (ok) DispatchAndSend(result);
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
                // Re-check under the lock — see design notes §3.3.
                if (node_.state().role != Role::kLeader && election_timer_.has_expired(now)) {
                    const auto durable =
                        node_.Handle(config_.self_id, ElectionTimeout{}, cluster_size_);
                    if (durable.persisted) {
                        election_timer_.reset(now);
                        result = durable.result;
                        ok = true;
                    }
                }
            }
            if (ok) DispatchAndSend(result);
        }
    }
}

PersistentRaftNode::DurableProposeResult RaftNode::ProposePut(const std::string& key,
                                                               const std::string& value) {
    PersistentRaftNode::DurableProposeResult durable;
    HandleResult to_send;
    bool should_send = false;
    {
        std::lock_guard<std::mutex> lock(node_mutex_);
        durable = node_.ProposePut(key, value);
        if (durable.persisted && durable.result.accepted) {
            to_send = durable.result.to_send;
            should_send = true;
        }
    }
    if (should_send) DispatchAndSend(to_send);
    return durable;
}

PersistentRaftNode::DurableProposeResult RaftNode::ProposeDelete(const std::string& key) {
    PersistentRaftNode::DurableProposeResult durable;
    HandleResult to_send;
    bool should_send = false;
    {
        std::lock_guard<std::mutex> lock(node_mutex_);
        durable = node_.ProposeDelete(key);
        if (durable.persisted && durable.result.accepted) {
            to_send = durable.result.to_send;
            should_send = true;
        }
    }
    if (should_send) DispatchAndSend(to_send);
    return durable;
}

bool RaftNode::Get(const std::string& key, std::string* out_value) {
    std::lock_guard<std::mutex> lock(node_mutex_);
    return node_.Get(key, out_value);
}

Role RaftNode::CurrentRole() {
    std::lock_guard<std::mutex> lock(node_mutex_);
    return node_.state().role;
}

}  // namespace raft