#include "raft/state_machine.h"

#include <algorithm>

namespace raft {

const char* to_string(Role role) {
    switch (role) {
        case Role::kFollower:
            return "Follower";
        case Role::kCandidate:
            return "Candidate";
        case Role::kLeader:
            return "Leader";
    }
    return "Unknown";
}

TermUpdateResult maybe_update_term(RaftState& state, Term observed_term) {
    if (observed_term <= state.current_term) {
        return TermUpdateResult{/*stepped_down=*/false};
    }
    state.current_term = observed_term;
    state.role = Role::kFollower;
    state.voted_for.reset();
    state.votes_received.clear();
    return TermUpdateResult{/*stepped_down=*/true};
}

bool candidate_log_is_at_least_as_up_to_date(Term candidate_last_log_term,
                                             LogIndex candidate_last_log_index,
                                             Term our_last_log_term,
                                             LogIndex our_last_log_index) {
    if (candidate_last_log_term != our_last_log_term) {
        return candidate_last_log_term > our_last_log_term;
    }
    return candidate_last_log_index >= our_last_log_index;
}

namespace {

std::size_t majority(std::size_t cluster_size) {
    return cluster_size / 2 + 1;
}

// Step 7: advances commit_index to new_commit_index (a no-op if it isn't
// actually higher) and collects every entry between the old and new
// last_applied into result.to_apply, in order. Shared by both the
// leader's majority-based path and the follower's leaderCommit-adoption
// path — see design notes §2.4.
void advance_commit(RaftState& state, LogIndex new_commit_index,
                    HandleResult& result) {
    if (new_commit_index <= state.commit_index) return;
    state.commit_index = new_commit_index;
    while (state.last_applied < state.commit_index) {
        state.last_applied += 1;
        if (const auto entry = state.log.at(state.last_applied)) {
            result.to_apply.push_back(*entry);
        }
    }
}

// Step 7: the Figure 8 rule (design doc §6.2, design notes §2.1-§2.2).
// Only ever called on the leader.
void maybe_advance_leader_commit(RaftState& state, std::size_t cluster_size,
                                 HandleResult& result) {
    std::vector<LogIndex> positions;
    positions.push_back(state.log.last_index());  // the leader itself
    for (const auto& [peer, match] : state.match_index) {
        (void)peer;
        positions.push_back(match);
    }
    const std::size_t untracked =
        (cluster_size > 0 ? cluster_size - 1 : 0) - state.match_index.size();
    positions.insert(positions.end(), untracked, 0);

    std::sort(positions.begin(), positions.end(), std::greater<LogIndex>());
    const std::size_t need = majority(cluster_size);
    if (need == 0 || need > positions.size()) return;

    const LogIndex candidate = positions[need - 1];
    if (candidate > state.commit_index &&
        state.log.term_at(candidate) == state.current_term) {
        advance_commit(state, candidate, result);
    }
}

HandleResult on_election_timeout(RaftState& state) {
    HandleResult result;
    if (state.role == Role::kLeader) return result;

    state.current_term += 1;
    state.role = Role::kCandidate;
    state.voted_for = state.self_id;
    state.votes_received.clear();
    state.votes_received.insert(state.self_id);
    state.next_index.clear();
    state.match_index.clear();
    // commit_index/last_applied deliberately untouched — see design
    // notes §3.2.

    RequestVoteRequest req;
    req.term = state.current_term;
    req.candidate_id = state.self_id;
    req.last_log_index = state.log.last_index();
    req.last_log_term = state.log.last_term();

    result.messages.push_back(Outgoing{kBroadcast, req});
    return result;
}

HandleResult on_request_vote(RaftState& state, const RequestVoteRequest& req) {
    HandleResult result;
    maybe_update_term(state, req.term);

    RequestVoteResponse resp;
    resp.term = state.current_term;

    if (req.term < state.current_term) {
        resp.vote_granted = false;
        result.messages.push_back(Outgoing{req.candidate_id, resp});
        return result;
    }

    const bool already_voted_for_someone_else =
        state.voted_for.has_value() && *state.voted_for != req.candidate_id;
    const bool log_ok = candidate_log_is_at_least_as_up_to_date(
        req.last_log_term, req.last_log_index, state.log.last_term(),
        state.log.last_index());

    if (!already_voted_for_someone_else && log_ok) {
        state.voted_for = req.candidate_id;
        resp.vote_granted = true;
    } else {
        resp.vote_granted = false;
    }

    result.messages.push_back(Outgoing{req.candidate_id, resp});
    return result;
}

HandleResult on_request_vote_response(RaftState& state, NodeId from,
                                      const RequestVoteResponse& resp,
                                      std::size_t cluster_size) {
    HandleResult result;
    const TermUpdateResult term_update = maybe_update_term(state, resp.term);
    if (term_update.stepped_down) return result;
    if (state.role != Role::kCandidate) return result;
    if (resp.term != state.current_term) return result;

    if (resp.vote_granted) state.votes_received.insert(from);

    if (state.votes_received.size() >= majority(cluster_size)) {
        state.role = Role::kLeader;
        result.became_leader = true;

        AppendEntriesRequest heartbeat;
        heartbeat.term = state.current_term;
        heartbeat.leader_id = state.self_id;
        heartbeat.prev_log_index = state.log.last_index();
        heartbeat.prev_log_term = state.log.last_term();
        heartbeat.leader_commit = state.commit_index;  // Step 7: real value

        result.messages.push_back(Outgoing{kBroadcast, heartbeat});
    }
    return result;
}

HandleResult on_append_entries(RaftState& state,
                               const AppendEntriesRequest& req) {
    HandleResult result;
    maybe_update_term(state, req.term);

    AppendEntriesResponse resp;
    resp.term = state.current_term;

    if (req.term < state.current_term) {
        resp.success = false;
        result.messages.push_back(Outgoing{req.leader_id, resp});
        return result;
    }

    if (state.role == Role::kCandidate) {
        state.role = Role::kFollower;
        state.votes_received.clear();
    }

    if (req.prev_log_index != 0) {
        const bool missing = req.prev_log_index > state.log.last_index();
        const bool mismatched =
            !missing && state.log.term_at(req.prev_log_index) != req.prev_log_term;
        if (missing || mismatched) {
            resp.success = false;
            result.messages.push_back(Outgoing{req.leader_id, resp});
            return result;
        }
    }

    LogIndex next_index = req.prev_log_index + 1;
    for (const LogEntry& e : req.entries) {
        if (next_index <= state.log.last_index()) {
            if (state.log.term_at(next_index) != e.term) {
                state.log.truncate_from(next_index);
                result.truncated_to = next_index;
                state.log.append(e.term, e.command);
            }
        } else {
            state.log.append(e.term, e.command);
        }
        ++next_index;
    }

    // Step 7: adopt the leader's commit index, bounded by what THIS
    // message actually brought us (design notes §2.4).
    if (req.leader_commit > state.commit_index) {
        const LogIndex last_new_entry_index =
            req.prev_log_index + req.entries.size();
        const LogIndex new_commit =
            std::min(req.leader_commit, last_new_entry_index);
        advance_commit(state, new_commit, result);
    }

    resp.success = true;
    // Bug D fix: report exactly the prefix this request verified, not the
    // follower's own last index and not "one past" for an empty heartbeat.
    resp.match_index = req.prev_log_index + req.entries.size();
    result.messages.push_back(Outgoing{req.leader_id, resp});
    return result;
}

HandleResult on_append_entries_response(RaftState& state, NodeId from,
                                        const AppendEntriesResponse& resp,
                                        std::size_t cluster_size) {
    HandleResult result;
    const TermUpdateResult term_update = maybe_update_term(state, resp.term);
    if (term_update.stepped_down) return result;
    if (state.role != Role::kLeader) return result;
    if (resp.term != state.current_term) return result;

    if (state.next_index.find(from) == state.next_index.end()) {
        state.next_index[from] = state.log.last_index() + 1;
        state.match_index[from] = 0;
        return result;
    }

    LogIndex& next = state.next_index[from];
    LogIndex& match = state.match_index[from];

    if (resp.success) {
        // Bug D fix. Trust what the follower says it matched, never what we
        // guess the request carried. Clamp to our own log (a follower cannot
        // honestly match past it) and never move match backwards: acks can
        // arrive late, duplicated or reordered, and a stale one must not
        // undo progress.
        const LogIndex acked = std::min(resp.match_index, state.log.last_index());
        if (acked > match) match = acked;
        // next only moves forward on success; a failure is what moves it back.
        if (next < match + 1) next = match + 1;
    } else if (next > 1) {
        next -= 1;
    }

    if (next <= state.log.last_index()) {
        const auto entry = state.log.at(next);
        AppendEntriesRequest retry;
        retry.term = state.current_term;
        retry.leader_id = state.self_id;
        retry.prev_log_index = next - 1;
        retry.prev_log_term = state.log.term_at(next - 1);
        retry.entries = {*entry};
        retry.leader_commit = state.commit_index;  // Step 7: real value
        result.messages.push_back(Outgoing{from, retry});
    }

    // Step 7: recompute after every successful ack — a majority might
    // now satisfy the commit rule (design notes §2.1-§2.3).
    if (resp.success) {
        maybe_advance_leader_commit(state, cluster_size, result);
    }

    return result;
}

}  // namespace

HandleResult handle(RaftState& state, NodeId from, const Event& event,
                    std::size_t cluster_size) {
    return std::visit(
        [&](const auto& ev) -> HandleResult {
            using T = std::decay_t<decltype(ev)>;
            if constexpr (std::is_same_v<T, ElectionTimeout>) {
                return on_election_timeout(state);
            } else if constexpr (std::is_same_v<T, Received<RequestVoteRequest>>) {
                return on_request_vote(state, ev.message);
            } else if constexpr (std::is_same_v<T, Received<RequestVoteResponse>>) {
                return on_request_vote_response(state, from, ev.message,
                                                cluster_size);
            } else if constexpr (std::is_same_v<T, Received<AppendEntriesRequest>>) {
                return on_append_entries(state, ev.message);
            } else {
                return on_append_entries_response(state, from, ev.message,
                                                  cluster_size);
            }
        },
        event);
}

ProposeResult propose(RaftState& state, std::string command) {
    ProposeResult result;
    if (state.role != Role::kLeader) return result;

    const Term entry_term = state.current_term;
    const LogIndex prev_index = state.log.last_index();
    const Term prev_term = state.log.last_term();

    const LogIndex new_index = state.log.append(entry_term, command);
    result.accepted = true;
    result.index = new_index;

    AppendEntriesRequest req;
    req.term = state.current_term;
    req.leader_id = state.self_id;
    req.prev_log_index = prev_index;
    req.prev_log_term = prev_term;
    req.entries = {LogEntry{entry_term, std::move(command)}};
    req.leader_commit = state.commit_index;  // Step 7: real value

    result.to_send.messages.push_back(Outgoing{kBroadcast, req});
    return result;
}

}  // namespace raft