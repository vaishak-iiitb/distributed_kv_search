#pragma once

#include <cstdint>
#include <map>
#include <optional>
#include <set>
#include <string>
#include <type_traits>
#include <variant>
#include <vector>

#include "raft/log.h"
#include "raft/rpc.h"

namespace raft {

enum class Role {
    kFollower,
    kCandidate,
    kLeader,
};

const char* to_string(Role role);

inline constexpr NodeId kBroadcast = static_cast<NodeId>(-1);

struct RaftState {
    NodeId self_id = 0;
    Role role = Role::kFollower;
    Term current_term = 0;
    std::optional<NodeId> voted_for;

    Log log;

    std::set<NodeId> votes_received;

    std::map<NodeId, LogIndex> next_index;
    std::map<NodeId, LogIndex> match_index;

    // Step 7: volatile on every node (design doc §2.2 — never persisted).
    // commit_index: highest index known safe to apply (leader computes
    // it; followers adopt it from leaderCommit). last_applied: highest
    // index actually handed to the caller via HandleResult::to_apply.
    LogIndex commit_index = 0;
    LogIndex last_applied = 0;
};

struct TermUpdateResult {
    bool stepped_down = false;
};

TermUpdateResult maybe_update_term(RaftState& state, Term observed_term);

bool candidate_log_is_at_least_as_up_to_date(Term candidate_last_log_term,
                                             LogIndex candidate_last_log_index,
                                             Term our_last_log_term,
                                             LogIndex our_last_log_index);

struct ElectionTimeout {};

template <typename T>
struct Received {
    T message;
};

using Event = std::variant<ElectionTimeout, Received<RequestVoteRequest>,
                           Received<RequestVoteResponse>,
                           Received<AppendEntriesRequest>,
                           Received<AppendEntriesResponse>>;

struct Outgoing {
    NodeId to;
    Message message;
};

struct HandleResult {
    std::vector<Outgoing> messages;
    bool became_leader = false;
    std::optional<LogIndex> truncated_to;

    // Step 7: entries between the old and new last_applied, in order —
    // newly safe to apply to the state machine. Empty unless
    // commit_index advanced during this call. See design notes §3.1.
    std::vector<LogEntry> to_apply;
};

HandleResult handle(RaftState& state, NodeId from, const Event& event,
                    std::size_t cluster_size);

struct ProposeResult {
    bool accepted = false;
    LogIndex index = 0;
    HandleResult to_send;
};

ProposeResult propose(RaftState& state, std::string command);

}  // namespace raft