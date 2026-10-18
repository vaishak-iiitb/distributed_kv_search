#include "raft/persistent_node.h"

namespace raft {

PersistentRaftNode::PersistentRaftNode(NodeId self_id, std::string state_path,
                                       std::string log_path, std::string kv_data_dir)
    : term_store_(std::move(state_path)),
      log_store_(std::move(log_path)),
      kv_engine_(kv_data_dir) {
    state_.self_id = self_id;

    log_store_.Replay([this](Term term, const std::string& command) {
        state_.log.append(term, command);
    });

    if (const auto persisted = term_store_.Load()) {
        state_.current_term = persisted->current_term;
        state_.voted_for = persisted->voted_for;
    }

    // last_applied_seq_ starts empty on purpose — see design notes §3.2.
}

bool PersistentRaftNode::PersistTermVoteIfChanged(
    Term old_term, std::optional<NodeId> old_voted_for) {
    if (state_.current_term == old_term && state_.voted_for == old_voted_for) {
        return true;
    }
    PersistedTermVote to_save;
    to_save.current_term = state_.current_term;
    to_save.voted_for = state_.voted_for;
    return term_store_.Save(to_save);
}

bool PersistentRaftNode::PersistNewLogEntries(LogIndex old_log_size) {
    for (LogIndex i = old_log_size + 1; i <= state_.log.last_index(); ++i) {
        const auto entry = state_.log.at(i);
        if (!entry || !log_store_.Append(entry->term, entry->command)) {
            return false;
        }
    }
    return true;
}

void PersistentRaftNode::ApplyCommitted(const std::vector<LogEntry>& to_apply) {
    for (const LogEntry& entry : to_apply) {
        if (is_noop_command(entry.command)) continue;  // leader's new-term no-op: nothing to apply
        const auto cmd = decode_command(entry.command);
        if (!cmd) continue;  // malformed — see Step 7b design notes §3.3

        // Step 9a: apply-time dedup (design notes §2.1/§2.2). A request
        // whose sequence_num is <= the last one we've already applied
        // for this client is a retry of something already in effect —
        // skip it, do not call Put/Delete again.
        auto& last_seq = last_applied_seq_[cmd->client_id];  // 0 if first-ever
        if (cmd->sequence_num <= last_seq && last_applied_seq_.count(cmd->client_id) > 0 &&
            last_seq != 0) {
            continue;
        }
        // Note: a fresh client's very first request legitimately has
        // last_seq == 0 after map default-construction, which could
        // collide with a (disallowed) sequence_num of 0 — client
        // sequence numbers are expected to start at 1, matching
        // "monotonically increasing" starting from a real first value
        // (design notes §2.3); sequence_num 0 is never a valid request.

        if (cmd->type == CommandType::kPut) {
            kv_engine_.Put(cmd->key, cmd->value);
        } else {
            kv_engine_.Delete(cmd->key);
        }
        last_seq = cmd->sequence_num;
    }
}

PersistentRaftNode::DurableResult PersistentRaftNode::Handle(
    NodeId from, const Event& event, std::size_t cluster_size) {
    const Term old_term = state_.current_term;
    const std::optional<NodeId> old_voted_for = state_.voted_for;
    const LogIndex old_log_size = state_.log.last_index();

    HandleResult result = handle(state_, from, event, cluster_size);

    if (!PersistTermVoteIfChanged(old_term, old_voted_for)) {
        return DurableResult{HandleResult{}, /*persisted=*/false};
    }

    if (result.truncated_to.has_value()) {
        if (!log_store_.Rewrite(state_.log)) {
            return DurableResult{HandleResult{}, /*persisted=*/false};
        }
    } else if (!PersistNewLogEntries(old_log_size)) {
        return DurableResult{HandleResult{}, /*persisted=*/false};
    }

    ApplyCommitted(result.to_apply);

    return DurableResult{std::move(result), /*persisted=*/true};
}

PersistentRaftNode::DurableProposeResult PersistentRaftNode::Propose(std::string command) {
    const LogIndex old_log_size = state_.log.last_index();

    ProposeResult result = propose(state_, std::move(command));
    if (!result.accepted) {
        return DurableProposeResult{std::move(result), /*persisted=*/true};
    }

    if (!PersistNewLogEntries(old_log_size)) {
        return DurableProposeResult{ProposeResult{}, /*persisted=*/false};
    }

    ApplyCommitted(result.to_send.to_apply);

    return DurableProposeResult{std::move(result), /*persisted=*/true};
}

PersistentRaftNode::DurableProposeResult PersistentRaftNode::ProposePut(
    ClientId client_id, SequenceNum sequence_num, const std::string& key,
    const std::string& value) {
    Command cmd;
    cmd.type = CommandType::kPut;
    cmd.client_id = client_id;
    cmd.sequence_num = sequence_num;
    cmd.key = key;
    cmd.value = value;
    return Propose(encode_command(cmd));
}

PersistentRaftNode::DurableProposeResult PersistentRaftNode::ProposeDelete(
    ClientId client_id, SequenceNum sequence_num, const std::string& key) {
    Command cmd;
    cmd.type = CommandType::kDelete;
    cmd.client_id = client_id;
    cmd.sequence_num = sequence_num;
    cmd.key = key;
    return Propose(encode_command(cmd));
}

PersistentRaftNode::DurableProposeResult PersistentRaftNode::ProposeNoop() {
    return Propose(std::string(kNoopCommand));
}

bool PersistentRaftNode::Get(const std::string& key, std::string* out_value) {
    return kv_engine_.Get(key, out_value);
}

}  // namespace raft