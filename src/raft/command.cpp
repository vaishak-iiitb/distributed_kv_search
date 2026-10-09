#include "raft/command.h"

namespace raft {
namespace {

void write_u32(std::string& out, std::uint32_t v) {
    for (int i = 0; i < 4; ++i) out.push_back(static_cast<char>((v >> (8 * i)) & 0xFF));
}

void write_u64(std::string& out, std::uint64_t v) {
    for (int i = 0; i < 8; ++i) out.push_back(static_cast<char>((v >> (8 * i)) & 0xFF));
}

bool read_u32(const std::string& in, std::size_t& pos, std::uint32_t& out) {
    if (in.size() - pos < 4) return false;
    std::uint32_t v = 0;
    for (int i = 0; i < 4; ++i) {
        v |= static_cast<std::uint32_t>(static_cast<unsigned char>(in[pos + i])) << (8 * i);
    }
    pos += 4;
    out = v;
    return true;
}

bool read_u64(const std::string& in, std::size_t& pos, std::uint64_t& out) {
    if (in.size() - pos < 8) return false;
    std::uint64_t v = 0;
    for (int i = 0; i < 8; ++i) {
        v |= static_cast<std::uint64_t>(static_cast<unsigned char>(in[pos + i])) << (8 * i);
    }
    pos += 8;
    out = v;
    return true;
}

}  // namespace

std::string encode_command(const Command& cmd) {
    std::string out;
    out.push_back(static_cast<char>(cmd.type));
    write_u64(out, cmd.client_id);
    write_u64(out, cmd.sequence_num);
    write_u32(out, static_cast<std::uint32_t>(cmd.key.size()));
    out.append(cmd.key);
    write_u32(out, static_cast<std::uint32_t>(cmd.value.size()));
    out.append(cmd.value);
    return out;
}

std::optional<Command> decode_command(const std::string& bytes) {
    if (bytes.empty()) return std::nullopt;

    const auto type_byte = static_cast<unsigned char>(bytes[0]);
    if (type_byte != static_cast<unsigned char>(CommandType::kPut) &&
        type_byte != static_cast<unsigned char>(CommandType::kDelete)) {
        return std::nullopt;
    }

    std::size_t pos = 1;

    std::uint64_t client_id = 0;
    if (!read_u64(bytes, pos, client_id)) return std::nullopt;

    std::uint64_t sequence_num = 0;
    if (!read_u64(bytes, pos, sequence_num)) return std::nullopt;

    std::uint32_t key_len = 0;
    if (!read_u32(bytes, pos, key_len)) return std::nullopt;
    if (bytes.size() - pos < key_len) return std::nullopt;
    std::string key = bytes.substr(pos, key_len);
    pos += key_len;

    std::uint32_t value_len = 0;
    if (!read_u32(bytes, pos, value_len)) return std::nullopt;
    if (bytes.size() - pos < value_len) return std::nullopt;
    std::string value = bytes.substr(pos, value_len);
    pos += value_len;

    if (pos != bytes.size()) return std::nullopt;

    Command cmd;
    cmd.type = static_cast<CommandType>(type_byte);
    cmd.client_id = client_id;
    cmd.sequence_num = sequence_num;
    cmd.key = std::move(key);
    cmd.value = std::move(value);
    return cmd;
}

}  // namespace raft