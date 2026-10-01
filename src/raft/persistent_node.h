#pragma once

#include <optional>
#include <string>

#include "raft/persistence.h"
#include "raft/state_machine.h"
#include "storage/kv_engine.h"

namespace raft {

class PersistentRaftNode {
public:
    // `kv_data_dir` is passed straight through to KVEngine's own
    // constructor (Phase 1) — a directory separate from state_path/
    // log_path, since KVEngine manages its own WAL/SSTable files there,
    // fully independently of Raft's own persistence (Step 5).
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

    // Convenience wrappers over Propose() + encode_command — see design
    // notes §3.1 for why command encoding lives outside Raft's own
    // types, and why these exist anyway for ergonomics.
    DurableProposeResult ProposePut(const std::string& key, const std::string& value);
    DurableProposeResult ProposeDelete(const std::string& key);

    // Raw local read of whatever has been applied on THIS node so far.
    // NOT linearizable — see design notes §2.3. Real Get, with
    // linearizability, is Step 9's scope.
    bool Get(const std::string& key, std::string* out_value);

private:
    bool PersistTermVoteIfChanged(Term old_term, std::optional<NodeId> old_voted_for);
    bool PersistNewLogEntries(LogIndex old_log_size);
    void ApplyCommitted(const std::vector<LogEntry>& to_apply);

    RaftState state_;
    TermVoteStore term_store_;
    PersistedLogStore log_store_;
    KVEngine kv_engine_;
};

}  // namespace raft