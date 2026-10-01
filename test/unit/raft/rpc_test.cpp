#include "raft/rpc.h"

#include <cstdint>
#include <cstdio>
#include <limits>
#include <random>
#include <string>
#include <string_view>

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

raft::Message sample_append_with_entries() {
    raft::AppendEntriesRequest m;
    m.term = 7;
    m.leader_id = 2;
    m.prev_log_index = 10;
    m.prev_log_term = 6;
    m.entries = {{6, "PUT a 1"}, {7, "DEL b"}};
    m.leader_commit = 9;
    return m;
}

// Round-trip helper: encode, decode, and check that the message is preserved
// and the whole frame was consumed.
void expect_roundtrip(const raft::Message& m) {
    const std::string frame = raft::encode_frame(m);
    const raft::DecodeResult r = raft::decode_frame(frame);
    CHECK(r.status == raft::DecodeStatus::kOk);
    CHECK(r.consumed == frame.size());
    CHECK(r.message.has_value());
    if (r.message) CHECK(*r.message == m);
}

// ---------------------------------------------------------------------------
// Round-trip tests
// ---------------------------------------------------------------------------

void test_roundtrip_request_vote() {
    expect_roundtrip(raft::RequestVoteRequest{3, 1, 42, 2});
    expect_roundtrip(raft::RequestVoteRequest{0, 0, 0, 0});  // all-zero edge
}

void test_roundtrip_request_vote_response() {
    expect_roundtrip(raft::RequestVoteResponse{5, true});
    expect_roundtrip(raft::RequestVoteResponse{5, false});
}

void test_roundtrip_append_entries() {
    expect_roundtrip(sample_append_with_entries());

    raft::AppendEntriesRequest heartbeat;  // empty entries
    heartbeat.term = 4;
    heartbeat.leader_id = 1;
    expect_roundtrip(heartbeat);
}

void test_roundtrip_append_response() {
    expect_roundtrip(raft::AppendEntriesResponse{9, true});
    expect_roundtrip(raft::AppendEntriesResponse{9, false});
}

// Binary command bytes: embedded NUL, high bytes, and empty string. A codec
// that uses strlen()/C strings would corrupt these.
void test_roundtrip_binary_and_empty_commands() {
    raft::AppendEntriesRequest m;
    m.entries = {
        {1, std::string("a\0b\xff\x00", 5)},
        {1, ""},
        {2, std::string(1000, 'z')},
    };
    expect_roundtrip(m);
}

// Values near the top of the integer range catch width and endianness bugs
// that small values hide (e.g. only the low byte being written).
void test_roundtrip_extreme_values() {
    const std::uint64_t max64 = std::numeric_limits<std::uint64_t>::max();
    const std::uint32_t max32 = std::numeric_limits<std::uint32_t>::max();
    expect_roundtrip(raft::RequestVoteRequest{max64, max32, max64, max64});
    expect_roundtrip(raft::AppendEntriesRequest{max64, max32, max64, max64,
                                                {{max64, "x"}}, max64});
}

// ---------------------------------------------------------------------------
// Stream/framing tests
// ---------------------------------------------------------------------------

// Every strict prefix of a valid frame must report kNeedMore. A decoder that
// returns kCorrupt or kOk for a prefix will break real TCP reads, which
// deliver frames in arbitrary chunks.
void test_every_prefix_needs_more() {
    const std::string frame = raft::encode_frame(sample_append_with_entries());
    for (std::size_t n = 0; n < frame.size(); ++n) {
        const raft::DecodeResult r =
            raft::decode_frame(std::string_view(frame).substr(0, n));
        CHECK(r.status == raft::DecodeStatus::kNeedMore);
        CHECK(r.consumed == 0);
        CHECK(!r.message.has_value());
    }
}

// Two frames back to back: the first decode must consume exactly its own
// bytes, and the remainder must decode as the second message.
void test_concatenated_frames() {
    const raft::Message a = raft::RequestVoteResponse{3, true};
    const raft::Message b = sample_append_with_entries();
    const std::string stream = raft::encode_frame(a) + raft::encode_frame(b);

    const raft::DecodeResult first = raft::decode_frame(stream);
    CHECK(first.status == raft::DecodeStatus::kOk);
    CHECK(first.message.has_value() && *first.message == a);

    const std::string_view rest = std::string_view(stream).substr(first.consumed);
    const raft::DecodeResult second = raft::decode_frame(rest);
    CHECK(second.status == raft::DecodeStatus::kOk);
    CHECK(second.consumed == rest.size());
    CHECK(second.message.has_value() && *second.message == b);
}

// ---------------------------------------------------------------------------
// Corruption tests. Each one builds a specific malformed frame and checks it
// is rejected, not accepted or crashed on.
// ---------------------------------------------------------------------------

void test_unknown_type_is_corrupt() {
    std::string frame = raft::encode_frame(raft::RequestVoteResponse{1, true});
    frame[4] = static_cast<char>(99);  // type byte sits right after length
    CHECK(raft::decode_frame(frame).status == raft::DecodeStatus::kCorrupt);
}

void test_zero_length_is_corrupt() {
    const std::string frame(4, '\0');  // length = 0, no type byte
    CHECK(raft::decode_frame(frame).status == raft::DecodeStatus::kCorrupt);
}

void test_oversized_length_is_corrupt() {
    // Length = kMaxFramePayload + 1, with no payload at all. Must be rejected
    // immediately, not treated as "need more".
    std::string frame;
    const std::uint32_t too_big = raft::kMaxFramePayload + 1;
    for (int i = 0; i < 4; ++i) {
        frame.push_back(static_cast<char>((too_big >> (8 * i)) & 0xFFu));
    }
    CHECK(raft::decode_frame(frame).status == raft::DecodeStatus::kCorrupt);
}

// A bool byte of 2 is neither true nor false. Accepting it would let a
// bit-flip silently change a vote.
void test_bool_out_of_range_is_corrupt() {
    std::string frame = raft::encode_frame(raft::RequestVoteResponse{1, true});
    // Frame: length(4) type(1) term(8) granted(1). granted is at offset 13.
    frame[13] = 2;
    CHECK(raft::decode_frame(frame).status == raft::DecodeStatus::kCorrupt);
}

// Trailing bytes inside a frame: length claims one more byte than the fields
// use. Must be corrupt, since silently ignoring bytes hides framing bugs.
void test_trailing_bytes_are_corrupt() {
    std::string frame = raft::encode_frame(raft::RequestVoteResponse{1, true});
    frame[0] = static_cast<char>(frame[0] + 1);  // length += 1
    frame.push_back('x');
    CHECK(raft::decode_frame(frame).status == raft::DecodeStatus::kCorrupt);
}

// Allocation bomb: entry count = 0xFFFFFFFF but no entry bytes follow. Must be
// rejected without reserving memory for 4 billion entries.
void test_entry_count_bomb_is_corrupt() {
    std::string frame = raft::encode_frame(raft::AppendEntriesRequest{});
    // Frame: length(4) type(1) term(8) leader_id(4) prev_index(8) prev_term(8)
    // entry_count(4). entry_count starts at offset 4+1+28 = 33.
    for (int i = 0; i < 4; ++i) frame[33 + i] = static_cast<char>(0xFF);
    CHECK(raft::decode_frame(frame).status == raft::DecodeStatus::kCorrupt);
}

// Command-length bomb: the string length field claims 4 GB inside a frame that
// holds only a few bytes.
void test_string_length_bomb_is_corrupt() {
    raft::AppendEntriesRequest m;
    m.entries = {{1, "hi"}};
    std::string frame = raft::encode_frame(m);
    // entry_count at offset 33, entry term at 37..44, command length at 45..48.
    for (int i = 0; i < 4; ++i) frame[45 + i] = static_cast<char>(0xFF);
    CHECK(raft::decode_frame(frame).status == raft::DecodeStatus::kCorrupt);
}

// Length header lies about a shorter frame than the payload needs: the length
// says 5 bytes total, but a full RequestVoteRequest needs much more. Must not
// read past the declared length into the next frame's bytes.
void test_short_length_does_not_read_past_frame() {
    std::string frame = raft::encode_frame(raft::RequestVoteRequest{1, 2, 3, 4});
    // Shrink the declared length to 5 (type + 4 bytes), but keep the full buffer.
    frame[0] = 5;
    frame[1] = 0;
    frame[2] = 0;
    frame[3] = 0;
    CHECK(raft::decode_frame(frame).status == raft::DecodeStatus::kCorrupt);
}

// ---------------------------------------------------------------------------
// Fuzz-style robustness: random and mutated input must never crash, never
// report consumed > size, and never report kOk with an invalid message.
// ---------------------------------------------------------------------------

void test_random_bytes_do_not_crash() {
    std::mt19937 rng(12345);
    std::uniform_int_distribution<int> byte(0, 255);
    std::uniform_int_distribution<int> len(0, 64);

    for (int iter = 0; iter < 20000; ++iter) {
        std::string buf;
        const int n = len(rng);
        for (int i = 0; i < n; ++i) buf.push_back(static_cast<char>(byte(rng)));
        const raft::DecodeResult r = raft::decode_frame(buf);
        CHECK(r.consumed <= buf.size());
        if (r.status == raft::DecodeStatus::kOk) CHECK(r.message.has_value());
    }
}

void test_mutated_valid_frames_do_not_crash() {
    std::mt19937 rng(777);
    std::uniform_int_distribution<int> byte(0, 255);
    const std::string base = raft::encode_frame(sample_append_with_entries());
    std::uniform_int_distribution<std::size_t> pos(0, base.size() - 1);

    for (int iter = 0; iter < 20000; ++iter) {
        std::string buf = base;
        buf[pos(rng)] = static_cast<char>(byte(rng));
        const raft::DecodeResult r = raft::decode_frame(buf);
        CHECK(r.consumed <= buf.size());
        if (r.status == raft::DecodeStatus::kOk) CHECK(r.message.has_value());
    }
}

}  // namespace

int main() {
    test_roundtrip_request_vote();
    test_roundtrip_request_vote_response();
    test_roundtrip_append_entries();
    test_roundtrip_append_response();
    test_roundtrip_binary_and_empty_commands();
    test_roundtrip_extreme_values();

    test_every_prefix_needs_more();
    test_concatenated_frames();

    test_unknown_type_is_corrupt();
    test_zero_length_is_corrupt();
    test_oversized_length_is_corrupt();
    test_bool_out_of_range_is_corrupt();
    test_trailing_bytes_are_corrupt();
    test_entry_count_bomb_is_corrupt();
    test_string_length_bomb_is_corrupt();
    test_short_length_does_not_read_past_frame();

    test_random_bytes_do_not_crash();
    test_mutated_valid_frames_do_not_crash();

    if (g_failures == 0) {
        std::printf("rpc_test: all checks passed\n");
        return 0;
    }
    std::fprintf(stderr, "rpc_test: %d check(s) failed\n", g_failures);
    return 1;
}