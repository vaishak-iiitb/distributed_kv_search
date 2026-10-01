#pragma once

#include <cstdint>
#include <functional>
#include <optional>
#include <string>

#include "raft/log.h"
#include "raft/rpc.h"

namespace raft {

struct PersistedTermVote {
    Term current_term = 0;
    std::optional<NodeId> voted_for;
};

class TermVoteStore {
public:
    explicit TermVoteStore(std::string path);
    bool Save(const PersistedTermVote& state);
    std::optional<PersistedTermVote> Load() const;

private:
    std::string path_;
};

class PersistedLogStore {
public:
    explicit PersistedLogStore(std::string path);

    bool Append(Term term, const std::string& command);

    std::size_t Replay(
        const std::function<void(Term, const std::string&)>& callback) const;

    // Step 6: fully rewrites the on-disk log to match `log`, using the
    // same temp-file-then-atomic-rename pattern as TermVoteStore::Save.
    // Needed because an append-only file can't express a truncated
    // suffix any other way — see design notes §3.3 for the cost
    // tradeoff. Returns false on any I/O failure.
    bool Rewrite(const Log& log);

private:
    static std::uint32_t Checksum(Term term, const std::string& command);

    std::string path_;
};

}  // namespace raft