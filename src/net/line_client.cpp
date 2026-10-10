#include "net/line_client.h"

#include <arpa/inet.h>
#include <fcntl.h>
#include <netinet/in.h>
#include <poll.h>
#include <sys/socket.h>
#include <unistd.h>

#include <cerrno>
#include <chrono>
#include <cstddef>

namespace net {
namespace {

using Clock = std::chrono::steady_clock;

int RemainingMs(Clock::time_point deadline) {
    const auto left =
        std::chrono::duration_cast<std::chrono::milliseconds>(deadline - Clock::now()).count();
    return left < 0 ? 0 : static_cast<int>(left);
}

// Waits until `fd` is ready for `events` or the deadline passes. Returns true
// when poll() reports ANY readiness (including error/hangup bits) — the
// caller learns the specifics from the following getsockopt/send/recv.
bool WaitReady(int fd, short events, Clock::time_point deadline) {
    for (;;) {
        pollfd p{};
        p.fd = fd;
        p.events = events;
        const int rc = ::poll(&p, 1, RemainingMs(deadline));
        if (rc > 0) return true;
        if (rc == 0) return false;
        if (errno == EINTR) continue;
        return false;
    }
}

// Returns a connected NON-BLOCKING socket, or -1.
int ConnectWithDeadline(const std::string& host, int port, Clock::time_point deadline) {
    const std::string ip = (host == "localhost") ? "127.0.0.1" : host;

    sockaddr_in addr{};
    addr.sin_family = AF_INET;
    addr.sin_port = htons(static_cast<uint16_t>(port));
    if (::inet_pton(AF_INET, ip.c_str(), &addr.sin_addr) != 1) return -1;

    const int fd = ::socket(AF_INET, SOCK_STREAM, 0);
    if (fd < 0) return -1;

    const int flags = ::fcntl(fd, F_GETFL, 0);
    if (flags < 0 || ::fcntl(fd, F_SETFL, flags | O_NONBLOCK) < 0) {
        ::close(fd);
        return -1;
    }

    if (::connect(fd, reinterpret_cast<sockaddr*>(&addr), sizeof(addr)) < 0) {
        if (errno != EINPROGRESS) {
            ::close(fd);
            return -1;
        }
        // Non-blocking connect: completion is signalled as "writable";
        // SO_ERROR then says whether it actually succeeded.
        if (!WaitReady(fd, POLLOUT, deadline)) {
            ::close(fd);
            return -1;
        }
        int err = 0;
        socklen_t len = sizeof(err);
        if (::getsockopt(fd, SOL_SOCKET, SO_ERROR, &err, &len) < 0 || err != 0) {
            ::close(fd);
            return -1;
        }
    }
    return fd;
}

bool SendAll(int fd, const std::string& data, Clock::time_point deadline) {
    std::size_t sent = 0;
    while (sent < data.size()) {
        // MSG_NOSIGNAL: a peer that already closed must produce an error
        // return here, not a process-killing SIGPIPE (same lesson as Step 8a).
        const ssize_t n = ::send(fd, data.data() + sent, data.size() - sent, MSG_NOSIGNAL);
        if (n > 0) {
            sent += static_cast<std::size_t>(n);
            continue;
        }
        if (n < 0 && errno == EINTR) continue;
        if (n < 0 && (errno == EAGAIN || errno == EWOULDBLOCK)) {
            if (!WaitReady(fd, POLLOUT, deadline)) return false;
            continue;
        }
        return false;
    }
    return true;
}

}  // namespace

std::optional<std::string> LineClient::RoundTrip(const std::string& host, int port,
                                                 const std::string& request_line,
                                                 int timeout_ms) {
    const auto deadline = Clock::now() + std::chrono::milliseconds(timeout_ms);

    const int fd = ConnectWithDeadline(host, port, deadline);
    if (fd < 0) return std::nullopt;

    std::optional<std::string> result;
    if (SendAll(fd, request_line + "\n", deadline)) {
        std::string buffer;
        char chunk[4096];
        for (;;) {
            const std::size_t newline = buffer.find('\n');
            if (newline != std::string::npos) {
                std::string line = buffer.substr(0, newline);
                if (!line.empty() && line.back() == '\r') line.pop_back();
                result = std::move(line);
                break;
            }
            if (!WaitReady(fd, POLLIN, deadline)) break;
            const ssize_t n = ::recv(fd, chunk, sizeof(chunk), 0);
            if (n > 0) {
                buffer.append(chunk, static_cast<std::size_t>(n));
            } else if (n == 0) {
                break;  // peer closed before a complete line: not trustworthy
            } else if (errno == EINTR || errno == EAGAIN || errno == EWOULDBLOCK) {
                continue;
            } else {
                break;
            }
        }
    }
    ::close(fd);
    return result;
}

}  // namespace net