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
    cmd.client_id = 1;
    cmd.sequence_num = 1;
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
    cmd.client_id = 1;
    cmd.sequence_num = 1;
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
    cmd.client_id = 1;
    cmd.sequence_num = 1;
    cmd.key = std::string("k\0ey", 4);   // embedded NUL
    cmd.value = "";                       // empty value on a Put is legal

    const std::string bytes = raft::encode_command(cmd);
    const auto decoded = raft::decode_command(bytes);

    CHECK(decoded.has_value());
    CHECK(decoded->key.size() == 4);
    CHECK(decoded->value.empty());
}

void test_roundtrip_includes_client_id_and_sequence() {
    raft::Command cmd;
    cmd.type = raft::CommandType::kPut;
    cmd.client_id = 42;
    cmd.sequence_num = 7;
    cmd.key = "x";
    cmd.value = "1";

    const auto decoded = raft::decode_command(raft::encode_command(cmd));
    CHECK(decoded.has_value());
    CHECK(decoded->client_id == 42);
    CHECK(decoded->sequence_num == 7);
}

void test_different_client_ids_do_not_collide() {
    raft::Command a;
    a.type = raft::CommandType::kPut;
    a.client_id = 1;
    a.sequence_num = 1;
    a.key = "k";
    a.value = "v1";

    raft::Command b;
    b.type = raft::CommandType::kPut;
    b.client_id = 2;  // same seq, different client
    b.sequence_num = 1;
    b.key = "k";
    b.value = "v2";

    const auto da = raft::decode_command(raft::encode_command(a));
    const auto db = raft::decode_command(raft::encode_command(b));
    CHECK(da.has_value() && db.has_value());
    CHECK(da->client_id != db->client_id);
    CHECK(da->value != db->value);
}

void test_decode_empty_bytes_is_nullopt() {
    CHECK(!raft::decode_command("").has_value());
}

void test_decode_invalid_type_byte_is_nullopt() {
    raft::Command cmd;
    cmd.type = raft::CommandType::kPut;
    cmd.client_id = 1;
    cmd.sequence_num = 1;
    cmd.key = "a";
    cmd.value = "b";
    std::string bytes = raft::encode_command(cmd);
    bytes[0] = static_cast<char>(99);  // neither kPut(0) nor kDelete(1)
    CHECK(!raft::decode_command(bytes).has_value());
}

void test_decode_truncated_key_length_bomb_is_nullopt() {
    std::string bytes;
    bytes.push_back(static_cast<char>(raft::CommandType::kPut));
    // 8 bytes of client_id + 8 bytes of sequence_num, then a claimed 4 GB
    // key length with only these 4 bytes actually present.
    for (int i = 0; i < 16; ++i) bytes.push_back(static_cast<char>(0x00));
    bytes.push_back(static_cast<char>(0xFF));
    bytes.push_back(static_cast<char>(0xFF));
    bytes.push_back(static_cast<char>(0xFF));
    bytes.push_back(static_cast<char>(0xFF));
    CHECK(!raft::decode_command(bytes).has_value());
}

void test_decode_trailing_garbage_is_nullopt() {
    raft::Command cmd;
    cmd.type = raft::CommandType::kPut;
    cmd.client_id = 1;
    cmd.sequence_num = 1;
    cmd.key = "a";
    cmd.value = "b";
    std::string bytes = raft::encode_command(cmd);
    bytes.push_back('X');  // one extra byte beyond what the fields account for
    CHECK(!raft::decode_command(bytes).has_value());
}

}  // namespace

int main() {
    test_roundtrip_put();
    test_roundtrip_delete();
    test_roundtrip_binary_and_empty_key_value();
    test_roundtrip_includes_client_id_and_sequence();
    test_different_client_ids_do_not_collide();

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