#include "raft/rpc.h"

#include <stdexcept>
#include <utility>

namespace raft {
namespace {

constexpr std::uint8_t kTypeRequestVote = 1;
constexpr std::uint8_t kTypeRequestVoteResponse = 2;
constexpr std::uint8_t kTypeAppendEntries = 3;
constexpr std::uint8_t kTypeAppendEntriesResponse = 4;

// Smallest possible encoded LogEntry: u64 term + u32 command length.
// Used to bound the entry count before reserving memory.
constexpr std::size_t kMinEncodedEntryBytes = 12;

// ---------------------------------------------------------------------------
// Byte-level writer/reader. All integers are little-endian, written one byte
// at a time so the format does not depend on the host's byte order.
// ---------------------------------------------------------------------------

class Writer {
public:
    void u8(std::uint8_t v) { buf_.push_back(static_cast<char>(v)); }
    void u32(std::uint32_t v) { put_le(v, 4); }
    void u64(std::uint64_t v) { put_le(v, 8); }
    void boolean(bool v) { u8(v ? 1 : 0); }

    void str(std::string_view s) {
        if (s.size() > 0xFFFFFFFFu) {
            throw std::length_error("rpc string exceeds u32 length");
        }
        u32(static_cast<std::uint32_t>(s.size()));
        buf_.append(s);
    }

    std::string take() { return std::move(buf_); }

private:
    void put_le(std::uint64_t v, int bytes) {
        for (int i = 0; i < bytes; ++i) {
            buf_.push_back(static_cast<char>((v >> (8 * i)) & 0xFFu));
        }
    }

    std::string buf_;
};

class Reader {
public:
    explicit Reader(std::string_view data) : data_(data) {}

    std::size_t remaining() const { return data_.size() - pos_; }
    bool done() const { return pos_ == data_.size(); }

    bool u8(std::uint8_t& out) {
        if (remaining() < 1) return false;
        out = static_cast<std::uint8_t>(data_[pos_]);
        ++pos_;
        return true;
    }

    bool u32(std::uint32_t& out) {
        std::uint64_t v = 0;
        if (!get_le(v, 4)) return false;
        out = static_cast<std::uint32_t>(v);
        return true;
    }

    bool u64(std::uint64_t& out) { return get_le(out, 8); }

    bool boolean(bool& out) {
        std::uint8_t b = 0;
        if (!u8(b) || b > 1) return false;  // anything but 0/1 is corrupt
        out = (b == 1);
        return true;
    }

    bool str(std::string& out) {
        std::uint32_t n = 0;
        if (!u32(n) || remaining() < n) return false;
        out.assign(data_.substr(pos_, n));
        pos_ += n;
        return true;
    }

private:
    bool get_le(std::uint64_t& out, int bytes) {
        if (remaining() < static_cast<std::size_t>(bytes)) return false;
        std::uint64_t v = 0;
        for (int i = 0; i < bytes; ++i) {
            v |= static_cast<std::uint64_t>(static_cast<std::uint8_t>(data_[pos_ + i]))
                 << (8 * i);
        }
        pos_ += static_cast<std::size_t>(bytes);
        out = v;
        return true;
    }

    std::string_view data_;
    std::size_t pos_ = 0;
};

// ---------------------------------------------------------------------------
// Entries: count u32, then per entry: term u64, command string.
// ---------------------------------------------------------------------------

void write_entries(Writer& w, const std::vector<LogEntry>& entries) {
    w.u32(static_cast<std::uint32_t>(entries.size()));
    for (const LogEntry& e : entries) {
        w.u64(e.term);
        w.str(e.command);
    }
}

bool read_entries(Reader& r, std::vector<LogEntry>& out) {
    std::uint32_t count = 0;
    if (!r.u32(count)) return false;

    // Reject counts that cannot possibly fit in the remaining bytes. This runs
    // BEFORE reserve(), so a garbage count of 0xFFFFFFFF cannot force a huge
    // allocation.
    if (count > r.remaining() / kMinEncodedEntryBytes) return false;

    out.clear();
    out.reserve(count);
    for (std::uint32_t i = 0; i < count; ++i) {
        LogEntry e;
        if (!r.u64(e.term) || !r.str(e.command)) return false;
        out.push_back(std::move(e));
    }
    return true;
}

// ---------------------------------------------------------------------------
// Per-message encode/decode.
// ---------------------------------------------------------------------------

template <class... Ts>
struct Overloaded : Ts... {
    using Ts::operator()...;
};
template <class... Ts>
Overloaded(Ts...) -> Overloaded<Ts...>;

// Writes the payload and returns the type byte for this message.
std::uint8_t encode_body(const Message& msg, Writer& w) {
    return std::visit(
        Overloaded{
            [&w](const RequestVoteRequest& m) -> std::uint8_t {
                w.u64(m.term);
                w.u32(m.candidate_id);
                w.u64(m.last_log_index);
                w.u64(m.last_log_term);
                return kTypeRequestVote;
            },
            [&w](const RequestVoteResponse& m) -> std::uint8_t {
                w.u64(m.term);
                w.boolean(m.vote_granted);
                return kTypeRequestVoteResponse;
            },
            [&w](const AppendEntriesRequest& m) -> std::uint8_t {
                w.u64(m.term);
                w.u32(m.leader_id);
                w.u64(m.prev_log_index);
                w.u64(m.prev_log_term);
                write_entries(w, m.entries);
                w.u64(m.leader_commit);
                return kTypeAppendEntries;
            },
            [&w](const AppendEntriesResponse& m) -> std::uint8_t {
                w.u64(m.term);
                w.boolean(m.success);
                w.u64(m.match_index);
                return kTypeAppendEntriesResponse;
            },
        },
        msg);
}

// Decodes a payload of the given type. Returns nullopt if any field is
// malformed OR if bytes are left over after the message is fully read.
std::optional<Message> decode_body(std::uint8_t type, Reader& r) {
    std::optional<Message> out;

    switch (type) {
        case kTypeRequestVote: {
            RequestVoteRequest m;
            if (!r.u64(m.term) || !r.u32(m.candidate_id) ||
                !r.u64(m.last_log_index) || !r.u64(m.last_log_term)) {
                return std::nullopt;
            }
            out = m;
            break;
        }
        case kTypeRequestVoteResponse: {
            RequestVoteResponse m;
            if (!r.u64(m.term) || !r.boolean(m.vote_granted)) return std::nullopt;
            out = m;
            break;
        }
        case kTypeAppendEntries: {
            AppendEntriesRequest m;
            if (!r.u64(m.term) || !r.u32(m.leader_id) || !r.u64(m.prev_log_index) ||
                !r.u64(m.prev_log_term) || !read_entries(r, m.entries) ||
                !r.u64(m.leader_commit)) {
                return std::nullopt;
            }
            out = std::move(m);
            break;
        }
        case kTypeAppendEntriesResponse: {
            AppendEntriesResponse m;
            if (!r.u64(m.term) || !r.boolean(m.success) || !r.u64(m.match_index)) {
                return std::nullopt;
            }
            out = m;
            break;
        }
        default:
            return std::nullopt;  // unknown message type
    }

    if (!r.done()) return std::nullopt;  // trailing bytes inside the frame
    return out;
}

}  // namespace

std::string encode_frame(const Message& msg) {
    Writer body;
    const std::uint8_t type = encode_body(msg, body);
    std::string payload = body.take();

    // +1 for the type byte, which is counted in the length field.
    const std::size_t length = payload.size() + 1;
    if (length > kMaxFramePayload) {
        throw std::length_error("rpc frame exceeds kMaxFramePayload");
    }

    Writer header;
    header.u32(static_cast<std::uint32_t>(length));
    header.u8(type);

    std::string frame = header.take();
    frame += payload;
    return frame;
}

DecodeResult decode_frame(std::string_view buf) {
    if (buf.size() < kFrameHeaderBytes) {
        return {DecodeStatus::kNeedMore, std::nullopt, 0};
    }

    Reader header(buf.substr(0, kFrameHeaderBytes));
    std::uint32_t length = 0;
    header.u32(length);

    // Reject before waiting for bytes, so a corrupt length cannot make the
    // caller buffer up to 4 GB.
    if (length == 0 || length > kMaxFramePayload) {
        return {DecodeStatus::kCorrupt, std::nullopt, 0};
    }

    if (buf.size() - kFrameHeaderBytes < length) {
        return {DecodeStatus::kNeedMore, std::nullopt, 0};
    }

    std::string_view payload = buf.substr(kFrameHeaderBytes, length);
    const auto type = static_cast<std::uint8_t>(payload[0]);
    Reader body(payload.substr(1));

    std::optional<Message> msg = decode_body(type, body);
    if (!msg) {
        return {DecodeStatus::kCorrupt, std::nullopt, 0};
    }
    return {DecodeStatus::kOk, std::move(msg), kFrameHeaderBytes + length};
}

}  // namespace raft