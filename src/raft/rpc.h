#pragma once

#include <cstddef>
#include <cstdint>
#include <optional>
#include <string>
#include <string_view>
#include <variant>
#include <vector>

namespace raft {

using NodeId = std::uint32_t;
using Term = std::uint64_t;
using LogIndex = std::uint64_t;

// One entry of the replicated log. The index is implied by position on the
// wire (see the design notes), so it is not stored here.
// NOTE: this project pins C++17, where defaulted comparison operators
// (`= default` on operator==) are not available — that's a C++20 feature.
// Each struct below defines operator== explicitly instead. Keep these in
// sync by hand if you add a field.

struct LogEntry {
    Term term = 0;
    std::string command;

    bool operator==(const LogEntry& o) const {
        return term == o.term && command == o.command;
    }
};

struct RequestVoteRequest {
    Term term = 0;
    NodeId candidate_id = 0;
    LogIndex last_log_index = 0;
    Term last_log_term = 0;

    bool operator==(const RequestVoteRequest& o) const {
        return term == o.term && candidate_id == o.candidate_id &&
               last_log_index == o.last_log_index &&
               last_log_term == o.last_log_term;
    }
};

struct RequestVoteResponse {
    Term term = 0;
    bool vote_granted = false;

    bool operator==(const RequestVoteResponse& o) const {
        return term == o.term && vote_granted == o.vote_granted;
    }
};

struct AppendEntriesRequest {
    Term term = 0;
    NodeId leader_id = 0;
    LogIndex prev_log_index = 0;
    Term prev_log_term = 0;
    std::vector<LogEntry> entries;  // empty entries == heartbeat
    LogIndex leader_commit = 0;

    bool operator==(const AppendEntriesRequest& o) const {
        return term == o.term && leader_id == o.leader_id &&
               prev_log_index == o.prev_log_index &&
               prev_log_term == o.prev_log_term && entries == o.entries &&
               leader_commit == o.leader_commit;
    }
};

struct AppendEntriesResponse {
    Term term = 0;
    bool success = false;
    // Bug D fix. Meaningful only when success == true: the highest log index
    // this follower has VERIFIED to be identical to the leader's log, i.e.
    // prev_log_index + entries.size() of the request being answered. It is
    // NOT the follower's own last index (the follower may hold extra,
    // unverified entries beyond that point). An empty heartbeat therefore
    // reports prev_log_index, never prev_log_index + 1. The leader uses this
    // value instead of guessing what the request carried.
    // Always 0 on a failure reply.
    LogIndex match_index = 0;

    bool operator==(const AppendEntriesResponse& o) const {
        return term == o.term && success == o.success &&
               match_index == o.match_index;
    }
};

using Message = std::variant<RequestVoteRequest, RequestVoteResponse,
                             AppendEntriesRequest, AppendEntriesResponse>;

// Frame header is a u32 length. Largest payload (type byte + fields) we accept.
inline constexpr std::size_t kFrameHeaderBytes = 4;
inline constexpr std::uint32_t kMaxFramePayload = 64u * 1024u * 1024u;

// Serialize a message into one complete frame.
// Throws std::length_error if the message exceeds kMaxFramePayload.
std::string encode_frame(const Message& msg);

enum class DecodeStatus {
    kOk,        // a complete frame was decoded; `consumed` bytes are used
    kNeedMore,  // buffer holds a valid prefix; read more bytes and retry
    kCorrupt,   // bytes cannot be a valid frame; the stream is unrecoverable
};

struct DecodeResult {
    DecodeStatus status = DecodeStatus::kNeedMore;
    std::optional<Message> message;
    std::size_t consumed = 0;  // bytes used from the front of the buffer, kOk only
};

// Try to decode one frame from the front of `buf`.
// Never throws, never reads past `buf`, never allocates more than the
// frame's declared payload size.
DecodeResult decode_frame(std::string_view buf);

}  // namespace raft