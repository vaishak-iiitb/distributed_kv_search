#include "raft/log.h"

namespace raft {

LogIndex Log::append(Term term, std::string command) {
    entries_.push_back(LogEntry{term, std::move(command)});
    return static_cast<LogIndex>(entries_.size());
}

std::optional<LogEntry> Log::at(LogIndex index) const {
    if (index == 0 || index > entries_.size()) return std::nullopt;
    return entries_[index - 1];
}

Term Log::term_at(LogIndex index) const {
    const auto entry = at(index);
    return entry ? entry->term : 0;
}

Term Log::last_term() const {
    return entries_.empty() ? 0 : entries_.back().term;
}

void Log::truncate_from(LogIndex index) {
    if (index == 0 || index > entries_.size()) return;
    entries_.resize(index - 1);
}

}  // namespace raft