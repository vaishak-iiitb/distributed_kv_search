#pragma once

#include <cstdint>
#include <optional>
#include <string>

namespace raft {

enum class CommandType : std::uint8_t {
    kPut = 0,
    kDelete = 1,
};

using ClientId = std::uint64_t;
using SequenceNum = std::uint64_t;

struct Command {
    CommandType type = CommandType::kPut;

    // Step 9a: identifies the request that produced this command, for
    // apply-time deduplication (design doc §8.2) — see design notes
    // §2.1 for why apply-time, not propose-time.
    ClientId client_id = 0;
    SequenceNum sequence_num = 0;

    std::string key;
    std::string value;  // unused for kDelete
};

// Flag #2 fix: the entry a new leader appends at the start of its term. It is
// NOT a client command. Raft only lets a leader treat older entries as
// committed once an entry of ITS OWN term has been replicated to a majority;
// without this entry a freshly elected leader's commit index (and therefore
// its applied state) stays behind until some client happens to write.
// It is deliberately NOT a valid encoded Command, so decode_command() rejects
// it, and the apply path skips it explicitly (see PersistentRaftNode).
inline constexpr const char* kNoopCommand = "NOOP";
inline bool is_noop_command(const std::string& bytes) { return bytes == kNoopCommand; }

std::string encode_command(const Command& cmd);
std::optional<Command> decode_command(const std::string& bytes);

}  // namespace raft