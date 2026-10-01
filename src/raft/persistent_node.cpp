#include "raft/persistent_node.h"

#include "raft/command.h"

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

    // commit_index/last_applied deliberately start at 0, even though the
    // log we just replayed may contain entries that were committed
    // before a crash. Neither is persisted (design doc §2.2), and this
    // node has no way to know, from its log alone, which prefix was
    // actually committed. It rediscovers that normally — from the
    // current leader, or by winning an election itself. Whatever WAS
    // durably applied to kv_engine_ before the crash is already there;
    // see design notes §2.2 for why re-applying it again later is a
    // safe, idempotent no-op.
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
        const auto cmd = decode_command(entry.command);
        if (!cmd) {
            // Cannot have come from ProposePut/ProposeDelete — see
            // design notes §3.3. Skip defensively rather than corrupt
            // kv_engine_ or crash.
            continue;
        }
        if (cmd->type == CommandType::kPut) {
            kv_engine_.Put(cmd->key, cmd->value);
        } else {
            kv_engine_.Delete(cmd->key);
        }
        // A Put/Delete failure here is not surfaced — see design notes
        // §3.4 for why that's a deliberate, acceptable boundary.
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

    // Step 7b: apply after persistence succeeds — the entry is already
    // durable in the Raft log regardless of whether this local apply
    // succeeds (design notes §3.4).
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

    // propose() never advances commit_index in a real (>1 node) cluster
    // (Step 7a design notes §3.3), so this is normally a no-op — applied
    // anyway so the (out-of-scope) single-node case stays correct too.
    ApplyCommitted(result.to_send.to_apply);

    return DurableProposeResult{std::move(result), /*persisted=*/true};
}

PersistentRaftNode::DurableProposeResult PersistentRaftNode::ProposePut(
    const std::string& key, const std::string& value) {
    Command cmd;
    cmd.type = CommandType::kPut;
    cmd.key = key;
    cmd.value = value;
    return Propose(encode_command(cmd));
}

PersistentRaftNode::DurableProposeResult PersistentRaftNode::ProposeDelete(
    const std::string& key) {
    Command cmd;
    cmd.type = CommandType::kDelete;
    cmd.key = key;
    return Propose(encode_command(cmd));
}

bool PersistentRaftNode::Get(const std::string& key, std::string* out_value) {
    return kv_engine_.Get(key, out_value);
}

}  // namespace raft