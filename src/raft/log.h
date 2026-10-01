#pragma once

#include <cstddef>
#include <optional>
#include <string>
#include <vector>

#include "raft/rpc.h"

namespace raft {

class Log {
public:
    LogIndex append(Term term, std::string command);
    std::optional<LogEntry> at(LogIndex index) const;
    Term term_at(LogIndex index) const;
    LogIndex last_index() const { return entries_.size(); }
    Term last_term() const;
    std::size_t size() const { return entries_.size(); }

    // Step 6: removes every entry from `index` onward (inclusive),
    // keeping only [1, index-1]. index == 0 or index > last_index() is a
    // no-op — see design notes §2.2 for the safety argument that this
    // can only ever discard uncommitted entries.
    void truncate_from(LogIndex index);

private:
    std::vector<LogEntry> entries_;
};

}  // namespace raft