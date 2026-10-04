#include "raft/raft_transport.h"

#include <arpa/inet.h>
#include <netinet/in.h>
#include <sys/socket.h>
#include <unistd.h>

#include <algorithm>
#include <csignal>
#include <cstring>

namespace raft {
namespace {

void write_u32_le(std::string& out, std::uint32_t v) {
    for (int i = 0; i < 4; ++i) out.push_back(static_cast<char>((v >> (8 * i)) & 0xFF));
}

// Blocking read of exactly 4 bytes. False on EOF/error.
bool read_u32_le_blocking(int fd, std::uint32_t& out) {
    char buf[4];
    std::size_t got = 0;
    while (got < 4) {
        const ssize_t n = ::read(fd, buf + got, 4 - got);
        if (n <= 0) return false;
        got += static_cast<std::size_t>(n);
    }
    out = static_cast<std::uint32_t>(static_cast<unsigned char>(buf[0])) |
          (static_cast<std::uint32_t>(static_cast<unsigned char>(buf[1])) << 8) |
          (static_cast<std::uint32_t>(static_cast<unsigned char>(buf[2])) << 16) |
          (static_cast<std::uint32_t>(static_cast<unsigned char>(buf[3])) << 24);
    return true;
}

bool write_all(int fd, const char* data, std::size_t len) {
    std::size_t sent = 0;
    while (sent < len) {
        const ssize_t n = ::write(fd, data + sent, len - sent);
        if (n <= 0) return false;
        sent += static_cast<std::size_t>(n);
    }
    return true;
}

}  // namespace

RaftTransport::RaftTransport(NodeId self_id, int listen_port,
                             std::map<NodeId, PeerAddress> peers,
                             MessageHandler on_message, std::size_t thread_pool_size)
    : self_id_(self_id),
      listen_port_(listen_port),
      peers_(std::move(peers)),
      on_message_(std::move(on_message)),
      pool_(thread_pool_size) {}  // initialized last, matching declaration order

RaftTransport::~RaftTransport() { Stop(); }

bool RaftTransport::Start() {
    // Writing to a TCP socket after the peer has closed its end can
    // raise SIGPIPE, whose DEFAULT disposition terminates the entire
    // process immediately — not just the one failing write(). This is a
    // standard requirement for any code doing raw socket I/O without a
    // framework underneath it; missing it here surfaced exactly when a
    // node's peer disappeared mid-test (Step 8b's leader-failure test).
    // Safe to call repeatedly — it only sets a process-wide signal
    // disposition, idempotently, even with multiple RaftTransport
    // instances in one process (as in the test suite).
    std::signal(SIGPIPE, SIG_IGN);

    listen_fd_ = ::socket(AF_INET, SOCK_STREAM, 0);
    if (listen_fd_ < 0) return false;

    int opt = 1;
    ::setsockopt(listen_fd_, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt));

    sockaddr_in addr{};
    addr.sin_family = AF_INET;
    addr.sin_addr.s_addr = INADDR_ANY;
    addr.sin_port = htons(static_cast<uint16_t>(listen_port_));

    if (::bind(listen_fd_, reinterpret_cast<sockaddr*>(&addr), sizeof(addr)) < 0) {
        ::close(listen_fd_);
        listen_fd_ = -1;
        return false;
    }
    if (::listen(listen_fd_, 16) < 0) {
        ::close(listen_fd_);
        listen_fd_ = -1;
        return false;
    }

    running_ = true;
    accept_thread_ = std::thread(&RaftTransport::AcceptLoop, this);
    return true;
}

void RaftTransport::Stop() {
    if (!running_.exchange(false)) return;  // already stopped

    if (listen_fd_ >= 0) {
        ::shutdown(listen_fd_, SHUT_RDWR);
        ::close(listen_fd_);
        listen_fd_ = -1;
    }
    if (accept_thread_.joinable()) accept_thread_.join();

    // Unblock every inbound connection's blocked read — see design
    // notes §3.4 for why this is required, not optional, for clean
    // shutdown. shutdown() only, never close() here: each connection's
    // own HandleInboundConnection call closes itself once unblocked.
    {
        std::lock_guard<std::mutex> lock(inbound_mutex_);
        for (int fd : inbound_fds_) {
            ::shutdown(fd, SHUT_RDWR);
        }
    }

    {
        std::lock_guard<std::mutex> lock(outbound_mutex_);
        for (auto& [peer, fd] : outbound_fds_) {
            (void)peer;
            ::close(fd);
        }
        outbound_fds_.clear();
    }
}

void RaftTransport::AcceptLoop() {
    while (running_) {
        sockaddr_in client_addr{};
        socklen_t len = sizeof(client_addr);
        const int client_fd =
            ::accept(listen_fd_, reinterpret_cast<sockaddr*>(&client_addr), &len);
        if (client_fd < 0) {
            if (!running_) break;  // Stop() closed listen_fd_ — expected
            continue;              // transient error, keep accepting
        }
        pool_.Enqueue([this, client_fd] { HandleInboundConnection(client_fd); });
    }
}

void RaftTransport::HandleInboundConnection(int fd) {
    {
        std::lock_guard<std::mutex> lock(inbound_mutex_);
        inbound_fds_.push_back(fd);
    }

    // Handshake: the connecting peer's NodeId, 4 raw bytes, before any
    // Step 1 frame — see design notes §2.2 for why this is required.
    std::uint32_t peer_id_raw = 0;
    bool ok = read_u32_le_blocking(fd, peer_id_raw);
    const NodeId peer_id = static_cast<NodeId>(peer_id_raw);

    std::string buffer;
    char chunk[4096];
    while (ok && running_) {
        const ssize_t n = ::read(fd, chunk, sizeof(chunk));
        if (n <= 0) break;  // peer closed, or shutdown() from Stop()
        buffer.append(chunk, static_cast<std::size_t>(n));

        while (true) {
            const DecodeResult result = decode_frame(buffer);
            if (result.status == DecodeStatus::kNeedMore) break;
            if (result.status == DecodeStatus::kCorrupt) {
                ok = false;
                break;
            }
            if (result.message) on_message_(peer_id, *result.message);
            buffer.erase(0, result.consumed);
        }
    }

    // Remove from the tracked set BEFORE closing — see design notes
    // §3.4 for why that order specifically prevents Stop() from ever
    // acting on a reused file descriptor.
    {
        std::lock_guard<std::mutex> lock(inbound_mutex_);
        inbound_fds_.erase(std::remove(inbound_fds_.begin(), inbound_fds_.end(), fd),
                           inbound_fds_.end());
    }
    ::close(fd);
}

// Caller must already hold outbound_mutex_ for the full duration of the
// call — see design notes §3.5. This function never locks internally.
bool RaftTransport::ConnectLocked(NodeId to) {
    if (outbound_fds_.count(to)) return true;

    const auto it = peers_.find(to);
    if (it == peers_.end()) return false;  // unknown peer id

    const int fd = ::socket(AF_INET, SOCK_STREAM, 0);
    if (fd < 0) return false;

    sockaddr_in addr{};
    addr.sin_family = AF_INET;
    addr.sin_port = htons(static_cast<uint16_t>(it->second.port));
    if (::inet_pton(AF_INET, it->second.host.c_str(), &addr.sin_addr) <= 0) {
        ::close(fd);
        return false;
    }

    if (::connect(fd, reinterpret_cast<sockaddr*>(&addr), sizeof(addr)) < 0) {
        ::close(fd);
        return false;  // peer unreachable right now — Send() just drops
    }

    std::string preamble;
    write_u32_le(preamble, static_cast<std::uint32_t>(self_id_));
    if (!write_all(fd, preamble.data(), preamble.size())) {
        ::close(fd);
        return false;
    }

    outbound_fds_[to] = fd;
    return true;
}

void RaftTransport::Send(NodeId to, const Message& message) {
    pool_.Enqueue([this, to, message] {
        // Held for the ENTIRE connect+write sequence, not just the fd
        // lookup — see design notes §3.5. This is what prevents two
        // concurrent Send() tasks from interleaving their writes on the
        // same socket, which otherwise corrupts the frame stream at the
        // byte level (not merely reordering it). The cost: every
        // outbound send on this node, even to different peers, is
        // serialized through one lock — an acceptable tradeoff at this
        // project's scale (see design notes §4).
        std::lock_guard<std::mutex> lock(outbound_mutex_);

        if (!ConnectLocked(to)) return;  // dropped — see design notes §2.4

        const int fd = outbound_fds_.at(to);
        const std::string frame = encode_frame(message);
        if (!write_all(fd, frame.data(), frame.size())) {
            ::close(fd);
            outbound_fds_.erase(to);  // next Send() to this peer re-dials
        }
    });
}

}  // namespace raft