#include "raft/command.h"

#include <cstdio>

namespace {

int g_failures = 0;

#define CHECK(cond)                                                          \
    do {                                                                     \
        if (!(cond)) {                                                       \
            std::fprintf(stderr, "%s:%d: CHECK failed: %s\n", __FILE__,      \
                         __LINE__, #cond);                                   \
            ++g_failures;                                                    \
        }                                                                    \
    } while (0)

void test_roundtrip_put() {
    raft::Command cmd;
    cmd.type = raft::CommandType::kPut;
    cmd.key = "hello";
    cmd.value = "world";

    const std::string bytes = raft::encode_command(cmd);
    const auto decoded = raft::decode_command(bytes);

    CHECK(decoded.has_value());
    CHECK(decoded->type == raft::CommandType::kPut);
    CHECK(decoded->key == "hello");
    CHECK(decoded->value == "world");
}

void test_roundtrip_delete() {
    raft::Command cmd;
    cmd.type = raft::CommandType::kDelete;
    cmd.key = "gone";
    // value deliberately left empty — unused for kDelete per design notes §2.1

    const std::string bytes = raft::encode_command(cmd);
    const auto decoded = raft::decode_command(bytes);

    CHECK(decoded.has_value());
    CHECK(decoded->type == raft::CommandType::kDelete);
    CHECK(decoded->key == "gone");
}

void test_roundtrip_binary_and_empty_key_value() {
    raft::Command cmd;
    cmd.type = raft::CommandType::kPut;
    cmd.key = std::string("k\0ey", 4);   // embedded NUL
    cmd.value = "";                       // empty value on a Put is legal

    const std::string bytes = raft::encode_command(cmd);
    const auto decoded = raft::decode_command(bytes);

    CHECK(decoded.has_value());
    CHECK(decoded->key.size() == 4);
    CHECK(decoded->value.empty());
}

void test_decode_empty_bytes_is_nullopt() {
    CHECK(!raft::decode_command("").has_value());
}

void test_decode_invalid_type_byte_is_nullopt() {
    std::string bytes = raft::encode_command(raft::Command{raft::CommandType::kPut, "a", "b"});
    bytes[0] = static_cast<char>(99);  // neither kPut(0) nor kDelete(1)
    CHECK(!raft::decode_command(bytes).has_value());
}

void test_decode_truncated_key_length_bomb_is_nullopt() {
    std::string bytes;
    bytes.push_back(static_cast<char>(raft::CommandType::kPut));
    // Claim a 4 GB key with only these 4 bytes actually present.
    bytes.push_back(static_cast<char>(0xFF));
    bytes.push_back(static_cast<char>(0xFF));
    bytes.push_back(static_cast<char>(0xFF));
    bytes.push_back(static_cast<char>(0xFF));
    CHECK(!raft::decode_command(bytes).has_value());
}

void test_decode_trailing_garbage_is_nullopt() {
    std::string bytes = raft::encode_command(raft::Command{raft::CommandType::kPut, "a", "b"});
    bytes.push_back('X');  // one extra byte beyond what the fields account for
    CHECK(!raft::decode_command(bytes).has_value());
}

}  // namespace

int main() {
    test_roundtrip_put();
    test_roundtrip_delete();
    test_roundtrip_binary_and_empty_key_value();

    test_decode_empty_bytes_is_nullopt();
    test_decode_invalid_type_byte_is_nullopt();
    test_decode_truncated_key_length_bomb_is_nullopt();
    test_decode_trailing_garbage_is_nullopt();

    if (g_failures == 0) {
        std::printf("command_test: all checks passed\n");
        return 0;
    }
    std::fprintf(stderr, "command_test: %d check(s) failed\n", g_failures);
    return 1;
}