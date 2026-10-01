#pragma once

#include <cstdint>
#include <optional>
#include <string>

namespace raft {

enum class CommandType : std::uint8_t {
    kPut = 0,
    kDelete = 1,
};

struct Command {
    CommandType type = CommandType::kPut;
    std::string key;
    std::string value;  // unused for kDelete — see design notes §2.1
};

// Encodes a Command into the opaque bytes a LogEntry's `command` field
// carries. This is the only place that knows what those bytes mean —
// state_machine.h/.cpp treat `command` as fully opaque by design (Step 4
// design notes §2.3/§3.1), and that stays true: this module sits ABOVE
// Raft, not inside it.
std::string encode_command(const Command& cmd);

// Nullopt on any malformed input. No checksum here — the bytes are
// already protected by PersistedLogStore's checksum (Step 5) and Raft's
// own consistency checks by the time anything calls this — see design
// notes §2.1.
std::optional<Command> decode_command(const std::string& bytes);

}  // namespace raft