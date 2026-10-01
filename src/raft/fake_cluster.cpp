#include "raft/fake_cluster.h"

#include <algorithm>
#include <cassert>

namespace raft {

FakeCluster::FakeCluster(std::size_t node_count, std::uint64_t seed)
    : nodes_(node_count), rng_(seed) {
    for (std::size_t i = 0; i < node_count; ++i) {
        nodes_[i].self_id = static_cast<NodeId>(i);
    }
}

void FakeCluster::inject(NodeId target, NodeId from, const Event& event) {
    const HandleResult result =
        handle(nodes_.at(target), from, event, nodes_.size());
    enqueue_outputs(target, result);
}

ProposeResult FakeCluster::propose(NodeId leader, std::string command) {
    ProposeResult result = raft::propose(nodes_.at(leader), std::move(command));
    if (result.accepted) {
        enqueue_outputs(leader, result.to_send);
    }
    return result;
}

void FakeCluster::enqueue_outputs(NodeId sender, const HandleResult& result) {
    for (const Outgoing& out : result.messages) {
        if (out.to == kBroadcast) {
            for (std::size_t i = 0; i < nodes_.size(); ++i) {
                const NodeId peer = static_cast<NodeId>(i);
                if (peer == sender) continue;
                queue_.push_back(InFlight{sender, peer, out.message});
            }
        } else {
            queue_.push_back(InFlight{sender, out.to, out.message});
        }
    }
}

void FakeCluster::deliver_at(std::size_t index) {
    assert(index < queue_.size());
    const InFlight in_flight = queue_[index];
    queue_.erase(queue_.begin() + static_cast<std::ptrdiff_t>(index));

    const Event event = std::visit(
        [](const auto& msg) -> Event {
            using T = std::decay_t<decltype(msg)>;
            return Received<T>{msg};
        },
        in_flight.message);

    const HandleResult result =
        handle(nodes_.at(in_flight.to), in_flight.from, event, nodes_.size());
    enqueue_outputs(in_flight.to, result);
}

bool FakeCluster::tick_front() {
    if (queue_.empty()) return false;
    deliver_at(0);
    return true;
}

bool FakeCluster::tick_random() {
    if (queue_.empty()) return false;
    std::uniform_int_distribution<std::size_t> pick(0, queue_.size() - 1);
    deliver_at(pick(rng_));
    return true;
}

std::size_t FakeCluster::drain_random(std::size_t max_ticks) {
    std::size_t delivered = 0;
    while (!queue_.empty() && delivered < max_ticks) {
        tick_random();
        ++delivered;
    }
    return delivered;
}

bool has_single_leader_for_latest_term(const FakeCluster& cluster) {
    Term latest_term = 0;
    for (std::size_t i = 0; i < cluster.node_count(); ++i) {
        latest_term =
            std::max(latest_term, cluster.node(static_cast<NodeId>(i)).current_term);
    }

    std::size_t leader_count = 0;
    for (std::size_t i = 0; i < cluster.node_count(); ++i) {
        const RaftState& s = cluster.node(static_cast<NodeId>(i));
        if (s.current_term != latest_term) continue;
        if (s.role == Role::kLeader) {
            ++leader_count;
        } else if (s.role == Role::kCandidate) {
            return false;
        }
    }
    return leader_count == 1;
}

bool logs_equal(const RaftState& a, const RaftState& b) {
    if (a.log.last_index() != b.log.last_index()) return false;
    for (LogIndex i = 1; i <= a.log.last_index(); ++i) {
        const auto ea = a.log.at(i);
        const auto eb = b.log.at(i);
        if (!ea || !eb) return false;  // shouldn't happen within range, but be safe
        if (ea->term != eb->term || ea->command != eb->command) return false;
    }
    return true;
}

}  // namespace raft