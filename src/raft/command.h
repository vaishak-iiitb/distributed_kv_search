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

std::string encode_command(const Command& cmd);
std::optional<Command> decode_command(const std::string& bytes);

}  // namespace raft