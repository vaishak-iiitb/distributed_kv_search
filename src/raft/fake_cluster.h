#pragma once

#include <cstdint>
#include <deque>
#include <random>
#include <string>
#include <vector>

#include "raft/state_machine.h"

namespace raft {

struct InFlight {
    NodeId from;
    NodeId to;
    Message message;
};

class FakeCluster {
public:
    FakeCluster(std::size_t node_count, std::uint64_t seed);

    std::size_t node_count() const { return nodes_.size(); }
    RaftState& node(NodeId id) { return nodes_.at(id); }
    const RaftState& node(NodeId id) const { return nodes_.at(id); }

    std::size_t queue_size() const { return queue_.size(); }
    bool queue_empty() const { return queue_.empty(); }

    void inject(NodeId target, NodeId from, const Event& event);

    // Step 4: a client write directed at `leader`. If `leader` isn't
    // actually Leader, this is a no-op beyond returning accepted=false —
    // mirrors calling raft::propose() directly, but also enqueues whatever
    // it produces, the same way inject() does for handle()'s output.
    ProposeResult propose(NodeId leader, std::string command);

    bool tick_front();
    bool tick_random();

    template <typename Pred>
    void tick_while_matching(Pred pred) {
        bool delivered_one = true;
        while (delivered_one && !queue_.empty()) {
            delivered_one = false;
            for (std::size_t i = 0; i < queue_.size(); ++i) {
                if (pred(queue_[i])) {
                    deliver_at(i);
                    delivered_one = true;
                    break;
                }
            }
        }
    }

    std::size_t drain_random(std::size_t max_ticks = 10000);

    template <typename Pred>
    bool drop_one(Pred pred) {
        for (std::size_t i = 0; i < queue_.size(); ++i) {
            if (pred(queue_[i])) {
                queue_.erase(queue_.begin() + static_cast<std::ptrdiff_t>(i));
                return true;
            }
        }
        return false;
    }

private:
    void deliver_at(std::size_t index);
    void enqueue_outputs(NodeId sender, const HandleResult& result);

    std::vector<RaftState> nodes_;
    std::deque<InFlight> queue_;
    std::mt19937_64 rng_;
};

bool has_single_leader_for_latest_term(const FakeCluster& cluster);

// Step 4: true iff `a` and `b` have identical logs — same length, and
// every entry's term and command match at every index. Used by
// replication tests to check followers actually converged with the
// leader, since there is no commit/apply signal yet to check instead
// (see design notes §4's table — that arrives in Step 7).
bool logs_equal(const RaftState& a, const RaftState& b);

}  // namespace raft