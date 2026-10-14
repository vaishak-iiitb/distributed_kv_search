#pragma once

// Test-only helpers for integration tests that run the REAL server binaries
// as child processes (and, in Phase 4, will kill/restart them on purpose).
// Header-only, POSIX (macOS + Linux).
//
//   harness::Process   fork/exec one binary, stdout+stderr -> a log file,
//                      Kill() = SIGKILL (a crash), Terminate() = SIGTERM then
//                      SIGKILL after a timeout (an orderly stop)
//   harness::PortIsFree   "can I bind this port right now?"
//   harness::WaitUntil    poll a predicate with a deadline
//
// Safety net: ~Process() terminates a still-running child, so a failing
// CHECK that returns early does not leak servers. A test that CRASHES (or is
// SIGKILLed) cannot run destructors and WILL leak children holding ports;
// the next run's port pre-check reports that (fix: `pkill kv_`).

#include <arpa/inet.h>
#include <fcntl.h>
#include <netinet/in.h>
#include <signal.h>
#include <sys/socket.h>
#include <sys/types.h>
#include <sys/wait.h>
#include <unistd.h>

#include <chrono>
#include <functional>
#include <string>
#include <thread>
#include <vector>

namespace harness {

inline bool WaitUntil(const std::function<bool()>& predicate, std::chrono::milliseconds timeout,
                      std::chrono::milliseconds poll = std::chrono::milliseconds(20)) {
    const auto deadline = std::chrono::steady_clock::now() + timeout;
    while (std::chrono::steady_clock::now() < deadline) {
        if (predicate()) return true;
        std::this_thread::sleep_for(poll);
    }
    return predicate();
}

// True if 127.0.0.1:port can be bound right now. SO_REUSEADDR is set so a
// port that is merely in TIME_WAIT counts as free (servers set it too), but a
// port with a live listener does not.
inline bool PortIsFree(int port) {
    const int fd = ::socket(AF_INET, SOCK_STREAM, 0);
    if (fd < 0) return false;
    int opt = 1;
    ::setsockopt(fd, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt));
    sockaddr_in addr{};
    addr.sin_family = AF_INET;
    addr.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
    addr.sin_port = htons(static_cast<uint16_t>(port));
    const bool ok = ::bind(fd, reinterpret_cast<sockaddr*>(&addr), sizeof(addr)) == 0;
    ::close(fd);
    return ok;
}

class Process {
public:
    // Terminate() return values besides a plain exit code (0..255) or
    // 128+signal for a child killed by a signal.
    static constexpr int kNotRunning = -2;
    static constexpr int kHadToKill = -1;  // ignored SIGTERM past the timeout

    Process(std::string name, std::string binary, std::vector<std::string> args,
            std::string log_path)
        : name_(std::move(name)),
          binary_(std::move(binary)),
          args_(std::move(args)),
          log_path_(std::move(log_path)) {}

    ~Process() { Terminate(3000); }

    Process(const Process&) = delete;
    Process& operator=(const Process&) = delete;

    const std::string& name() const { return name_; }
    const std::string& log_path() const { return log_path_; }

    // Starts (or restarts, after it has exited) the child. The log file is
    // opened in append mode, so a restart keeps the earlier output.
    bool Start() {
        if (pid_ > 0) return false;
        std::vector<char*> argv;
        argv.push_back(const_cast<char*>(binary_.c_str()));
        for (auto& a : args_) argv.push_back(const_cast<char*>(a.c_str()));
        argv.push_back(nullptr);

        const pid_t pid = ::fork();
        if (pid < 0) return false;
        if (pid == 0) {
            // Child: async-signal-safe calls only until exec.
            const int fd = ::open(log_path_.c_str(), O_WRONLY | O_CREAT | O_APPEND, 0644);
            if (fd >= 0) {
                ::dup2(fd, 1);
                ::dup2(fd, 2);
                if (fd > 2) ::close(fd);
            }
            ::execv(binary_.c_str(), argv.data());
            ::_exit(127);  // exec failed
        }
        pid_ = pid;
        exit_code_ = kNotRunning;
        return true;
    }

    // True while the child is alive. Reaps it (and records its exit code) if
    // it has exited on its own.
    bool Running() {
        if (pid_ <= 0) return false;
        int status = 0;
        const pid_t r = ::waitpid(pid_, &status, WNOHANG);
        if (r == 0) return true;
        if (r == pid_) exit_code_ = DecodeStatus(status);
        pid_ = -1;
        return false;
    }

    // Exit code of the last child that is gone (0..255, 128+signal, or
    // kNotRunning if it was never started / is still running).
    int exit_code() const { return exit_code_; }

    // Simulates a crash: SIGKILL, no cleanup runs in the child.
    void Kill() {
        if (pid_ <= 0) return;
        ::kill(pid_, SIGKILL);
        int status = 0;
        ::waitpid(pid_, &status, 0);
        exit_code_ = DecodeStatus(status);
        pid_ = -1;
    }

    // Orderly stop: SIGTERM, wait up to timeout_ms, then SIGKILL.
    int Terminate(int timeout_ms) {
        if (!Running()) return kNotRunning;
        ::kill(pid_, SIGTERM);
        const bool exited = WaitUntil([this] { return !Running(); },
                                      std::chrono::milliseconds(timeout_ms),
                                      std::chrono::milliseconds(10));
        if (exited) return exit_code_;
        Kill();
        return kHadToKill;
    }

private:
    static int DecodeStatus(int status) {
        if (WIFEXITED(status)) return WEXITSTATUS(status);
        if (WIFSIGNALED(status)) return 128 + WTERMSIG(status);
        return kNotRunning;
    }

    std::string name_, binary_;
    std::vector<std::string> args_;
    std::string log_path_;
    pid_t pid_ = -1;
    int exit_code_ = kNotRunning;
};

}  // namespace harness