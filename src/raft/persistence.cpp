#include "raft/persistence.h"

#include <cstdio>
#include <fstream>

#if !defined(_WIN32)
#include <unistd.h>
#endif

namespace raft {
namespace {

std::uint32_t Fnv1a(const void* data, std::size_t len, std::uint32_t seed) {
    const auto* bytes = static_cast<const unsigned char*>(data);
    std::uint32_t hash = seed;
    for (std::size_t i = 0; i < len; ++i) {
        hash ^= bytes[i];
        hash *= 16777619u;
    }
    return hash;
}

void WriteU32LE(std::ostream& out, std::uint32_t v) {
    for (int i = 0; i < 4; ++i) out.put(static_cast<char>((v >> (8 * i)) & 0xFF));
}
void WriteU64LE(std::ostream& out, std::uint64_t v) {
    for (int i = 0; i < 8; ++i) out.put(static_cast<char>((v >> (8 * i)) & 0xFF));
}
bool ReadU32LE(std::istream& in, std::uint32_t& out) {
    std::uint32_t v = 0;
    for (int i = 0; i < 4; ++i) {
        const int c = in.get();
        if (c == EOF) return false;
        v |= static_cast<std::uint32_t>(static_cast<unsigned char>(c)) << (8 * i);
    }
    out = v;
    return true;
}
bool ReadU64LE(std::istream& in, std::uint64_t& out) {
    std::uint64_t v = 0;
    for (int i = 0; i < 8; ++i) {
        const int c = in.get();
        if (c == EOF) return false;
        v |= static_cast<std::uint64_t>(static_cast<unsigned char>(c)) << (8 * i);
    }
    out = v;
    return true;
}

bool FsyncPath(const std::string& path) {
#if defined(_WIN32)
    (void)path;
    return true;
#else
    FILE* f = std::fopen(path.c_str(), "rb+");
    if (!f) return false;
    const bool ok = (std::fflush(f) == 0) && (::fsync(fileno(f)) == 0);
    std::fclose(f);
    return ok;
#endif
}

}  // namespace

TermVoteStore::TermVoteStore(std::string path) : path_(std::move(path)) {}

bool TermVoteStore::Save(const PersistedTermVote& state) {
    const std::string tmp_path = path_ + ".tmp";
    {
        std::ofstream out(tmp_path, std::ios::binary | std::ios::trunc);
        if (!out) return false;
        WriteU64LE(out, state.current_term);
        out.put(state.voted_for.has_value() ? 1 : 0);
        WriteU32LE(out, state.voted_for.value_or(0));
        out.flush();
        if (!out) return false;
    }
    if (!FsyncPath(tmp_path)) return false;
    if (std::rename(tmp_path.c_str(), path_.c_str()) != 0) return false;
    return true;
}

std::optional<PersistedTermVote> TermVoteStore::Load() const {
    std::ifstream in(path_, std::ios::binary);
    if (!in) return std::nullopt;

    PersistedTermVote state;
    std::uint64_t term = 0;
    if (!ReadU64LE(in, term)) return std::nullopt;
    state.current_term = term;

    const int has_vote = in.get();
    if (has_vote == EOF) return std::nullopt;

    std::uint32_t voted_for = 0;
    if (!ReadU32LE(in, voted_for)) return std::nullopt;

    if (has_vote == 1) state.voted_for = voted_for;
    return state;
}

PersistedLogStore::PersistedLogStore(std::string path) : path_(std::move(path)) {}

std::uint32_t PersistedLogStore::Checksum(Term term, const std::string& command) {
    std::uint32_t h = Fnv1a(&term, sizeof(term), 2166136261u);
    h = Fnv1a(command.data(), command.size(), h);
    return h;
}

bool PersistedLogStore::Append(Term term, const std::string& command) {
    {
        std::ofstream out(path_, std::ios::binary | std::ios::app);
        if (!out) return false;
        WriteU32LE(out, Checksum(term, command));
        WriteU64LE(out, term);
        WriteU32LE(out, static_cast<std::uint32_t>(command.size()));
        out.write(command.data(), static_cast<std::streamsize>(command.size()));
        out.flush();
        if (!out) return false;
    }
    return FsyncPath(path_);
}

bool PersistedLogStore::Rewrite(const Log& log) {
    const std::string tmp_path = path_ + ".tmp";
    {
        std::ofstream out(tmp_path, std::ios::binary | std::ios::trunc);
        if (!out) return false;
        for (LogIndex i = 1; i <= log.last_index(); ++i) {
            const auto entry = log.at(i);
            if (!entry) return false;  // shouldn't happen; log is self-consistent
            WriteU32LE(out, Checksum(entry->term, entry->command));
            WriteU64LE(out, entry->term);
            WriteU32LE(out, static_cast<std::uint32_t>(entry->command.size()));
            out.write(entry->command.data(),
                     static_cast<std::streamsize>(entry->command.size()));
        }
        out.flush();
        if (!out) return false;
    }
    if (!FsyncPath(tmp_path)) return false;
    if (std::rename(tmp_path.c_str(), path_.c_str()) != 0) return false;
    return true;
}

std::size_t PersistedLogStore::Replay(
    const std::function<void(Term, const std::string&)>& callback) const {
    std::ifstream in(path_, std::ios::binary);
    if (!in) return 0;

    std::size_t count = 0;
    while (true) {
        std::uint32_t stored_checksum = 0;
        if (!ReadU32LE(in, stored_checksum)) break;

        std::uint64_t term = 0;
        if (!ReadU64LE(in, term)) break;

        std::uint32_t command_len = 0;
        if (!ReadU32LE(in, command_len)) break;

        std::string command(command_len, '\0');
        if (command_len > 0) {
            in.read(command.data(), static_cast<std::streamsize>(command_len));
            if (static_cast<std::uint32_t>(in.gcount()) != command_len) break;
        }

        if (Checksum(term, command) != stored_checksum) break;

        callback(term, command);
        ++count;
    }
    return count;
}

}  // namespace raft