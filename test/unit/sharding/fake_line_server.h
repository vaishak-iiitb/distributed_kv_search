#pragma once

// Test-only: a scripted newline-protocol server on 127.0.0.1 (ephemeral
// port). Lets the client logic be tested with exact, deterministic server
// behaviour — redirects, timeouts, silence — without running any Raft.

#include <arpa/inet.h>
#include <netinet/in.h>
#include <poll.h>
#include <sys/socket.h>
#include <unistd.h>

#include <atomic>
#include <functional>
#include <mutex>
#include <optional>
#include <string>
#include <thread>
#include <vector>

class FakeLineServer {
public:
    // Return a response line (no trailing '\n'), or std::nullopt to stay
    // silent (the connection is held open until the client gives up).
    using Handler = std::function<std::optional<std::string>(const std::string& line)>;

    explicit FakeLineServer(Handler handler) : handler_(std::move(handler)) {
        listen_fd_ = ::socket(AF_INET, SOCK_STREAM, 0);
        int opt = 1;
        ::setsockopt(listen_fd_, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt));
        sockaddr_in addr{};
        addr.sin_family = AF_INET;
        addr.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
        addr.sin_port = 0;
        ::bind(listen_fd_, reinterpret_cast<sockaddr*>(&addr), sizeof(addr));
        ::listen(listen_fd_, 16);
        socklen_t len = sizeof(addr);
        ::getsockname(listen_fd_, reinterpret_cast<sockaddr*>(&addr), &len);
        port_ = ntohs(addr.sin_port);
        accept_thread_ = std::thread([this] { AcceptLoop(); });
    }

    ~FakeLineServer() {
        stop_ = true;
        if (accept_thread_.joinable()) accept_thread_.join();
        for (auto& t : conn_threads_) t.join();
        ::close(listen_fd_);
    }

    FakeLineServer(const FakeLineServer&) = delete;
    FakeLineServer& operator=(const FakeLineServer&) = delete;

    int port() const { return port_; }

    std::vector<std::string> received() const {
        std::lock_guard<std::mutex> lock(mu_);
        return received_;
    }

private:
    void AcceptLoop() {
        while (!stop_) {
            pollfd p{listen_fd_, POLLIN, 0};
            if (::poll(&p, 1, 20) <= 0) continue;
            const int fd = ::accept(listen_fd_, nullptr, nullptr);
            if (fd < 0) continue;
            conn_threads_.emplace_back([this, fd] { Serve(fd); });
        }
    }

    void Serve(int fd) {
        std::string buffer;
        bool answered = false;
        while (!stop_) {
            pollfd p{fd, POLLIN, 0};
            if (::poll(&p, 1, 20) <= 0) continue;
            char chunk[1024];
            const ssize_t n = ::read(fd, chunk, sizeof(chunk));
            if (n <= 0) break;  // client closed
            buffer.append(chunk, static_cast<std::size_t>(n));
            const std::size_t nl = buffer.find('\n');
            if (nl == std::string::npos || answered) continue;
            const std::string line = buffer.substr(0, nl);
            answered = true;
            {
                std::lock_guard<std::mutex> lock(mu_);
                received_.push_back(line);
            }
            const auto response = handler_(line);
            if (response.has_value()) {
                const std::string out = *response + "\n";
                ssize_t sent = ::send(fd, out.data(), out.size(), MSG_NOSIGNAL);
                (void)sent;
            }
        }
        ::close(fd);
    }

    Handler handler_;
    int listen_fd_ = -1;
    int port_ = 0;
    std::atomic<bool> stop_{false};
    std::thread accept_thread_;
    std::vector<std::thread> conn_threads_;
    mutable std::mutex mu_;
    std::vector<std::string> received_;
};

// A port nothing is listening on (bind, read the port, close).
inline int UnusedPort() {
    const int fd = ::socket(AF_INET, SOCK_STREAM, 0);
    sockaddr_in addr{};
    addr.sin_family = AF_INET;
    addr.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
    addr.sin_port = 0;
    ::bind(fd, reinterpret_cast<sockaddr*>(&addr), sizeof(addr));
    socklen_t len = sizeof(addr);
    ::getsockname(fd, reinterpret_cast<sockaddr*>(&addr), &len);
    const int port = ntohs(addr.sin_port);
    ::close(fd);
    return port;
}