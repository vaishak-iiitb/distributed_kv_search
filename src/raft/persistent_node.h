#pragma once

#include <map>
#include <optional>
#include <string>

#include "raft/command.h"
#include "raft/persistence.h"
#include "raft/state_machine.h"
#include "storage/kv_engine.h"

namespace raft {

class PersistentRaftNode {
public:
    PersistentRaftNode(NodeId self_id, std::string state_path, std::string log_path,
                       std::string kv_data_dir);

    const RaftState& state() const { return state_; }

    struct DurableResult {
        HandleResult result;
        bool persisted = true;
    };
    DurableResult Handle(NodeId from, const Event& event, std::size_t cluster_size);

    struct DurableProposeResult {
        ProposeResult result;
        bool persisted = true;
    };
    DurableProposeResult Propose(std::string command);

    // Step 9a: client_id/sequence_num identify the request for apply-
    // time deduplication (design notes §2.1/§3.2) — NOT checked here at
    // propose time (design notes §3.3); a retried request still appends
    // a new log entry, but will be a no-op when applied.
    DurableProposeResult ProposePut(ClientId client_id, SequenceNum sequence_num,
                                    const std::string& key, const std::string& value);
    DurableProposeResult ProposeDelete(ClientId client_id, SequenceNum sequence_num,
                                       const std::string& key);

    bool Get(const std::string& key, std::string* out_value);

private:
    bool PersistTermVoteIfChanged(Term old_term, std::optional<NodeId> old_voted_for);
    bool PersistNewLogEntries(LogIndex old_log_size);
    void ApplyCommitted(const std::vector<LogEntry>& to_apply);

    RaftState state_;
    TermVoteStore term_store_;
    PersistedLogStore log_store_;
    KVEngine kv_engine_;

    // Step 9a: last applied sequence number per client — volatile, not
    // persisted. See design notes §3.2 for why that's a bounded,
    // defensible simplification here, not a silent gap.
    std::map<ClientId, SequenceNum> last_applied_seq_;
};

}  // namespace raft