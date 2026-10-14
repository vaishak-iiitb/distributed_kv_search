// Tests for the process harness itself, using only /bin/sh and /bin/sleep.
// No cluster, no Raft, no network except one throwaway listening socket.

#include <arpa/inet.h>
#include <netinet/in.h>
#include <sys/socket.h>
#include <unistd.h>

#include <cstdio>
#include <filesystem>
#include <fstream>
#include <sstream>
#include <string>

#include "process_harness.h"

using harness::Process;
using harness::WaitUntil;
using std::chrono::milliseconds;

static int g_failures = 0;
#define CHECK(cond)                                                                       \
    do {                                                                                  \
        if (!(cond)) {                                                                    \
            std::fprintf(stderr, "%s:%d: CHECK failed: %s\n", __FILE__, __LINE__, #cond); \
            ++g_failures;                                                                 \
        }                                                                                 \
    } while (0)

namespace {

std::string ReadAll(const std::string& path) {
    std::ifstream in(path);
    std::ostringstream out;
    out << in.rdbuf();
    return out.str();
}

void TestKillAndRestart(const std::string& dir) {
    Process p("sleeper", "/bin/sleep", {"30"}, dir + "/sleeper.log");
    CHECK(!p.Running());
    CHECK(p.Start());
    CHECK(p.Running());
    CHECK(!p.Start());  // already running
    p.Kill();
    CHECK(!p.Running());
    CHECK(p.exit_code() == 128 + SIGKILL);
    CHECK(p.Start());  // restart after a kill works
    CHECK(p.Running());
    p.Kill();
}

void TestTerminate(const std::string& dir) {
    Process polite("polite", "/bin/sleep", {"30"}, dir + "/polite.log");
    CHECK(polite.Start());
    CHECK(polite.Terminate(2000) == 128 + SIGTERM);  // default SIGTERM action
    CHECK(!polite.Running());

    // A child that ignores SIGTERM must be escalated to SIGKILL.
    Process stubborn("stubborn", "/bin/sh",
                     {"-c", "trap '' TERM; while :; do sleep 0.1; done"}, dir + "/stubborn.log");
    CHECK(stubborn.Start());
    std::this_thread::sleep_for(milliseconds(200));  // let the trap install
    CHECK(stubborn.Terminate(300) == Process::kHadToKill);
    CHECK(!stubborn.Running());

    Process never("never", "/bin/sleep", {"1"}, dir + "/never.log");
    CHECK(never.Terminate(100) == Process::kNotRunning);  // not started
}

void TestLogsAndExitCodes(const std::string& dir) {
    Process talker("talker", "/bin/sh", {"-c", "echo out-line; echo err-line >&2; exit 3"},
                   dir + "/talker.log");
    CHECK(talker.Start());
    CHECK(WaitUntil([&] { return !talker.Running(); }, milliseconds(3000)));
    CHECK(talker.exit_code() == 3);
    const std::string log = ReadAll(dir + "/talker.log");
    CHECK(log.find("out-line") != std::string::npos);  // stdout captured
    CHECK(log.find("err-line") != std::string::npos);  // stderr captured

    CHECK(talker.Start());  // restart appends, does not truncate
    CHECK(WaitUntil([&] { return !talker.Running(); }, milliseconds(3000)));
    const std::string log2 = ReadAll(dir + "/talker.log");
    CHECK(log2.size() > log.size());

    Process missing("missing", "/nonexistent/binary", {}, dir + "/missing.log");
    CHECK(missing.Start());  // fork succeeds; exec fails inside the child
    CHECK(WaitUntil([&] { return !missing.Running(); }, milliseconds(3000)));
    CHECK(missing.exit_code() == 127);
}

void TestDestructorStopsChild(const std::string& dir) {
    pid_t observed = -1;
    {
        Process p("scoped", "/bin/sh", {"-c", "echo $$; exec sleep 30"}, dir + "/scoped.log");
        CHECK(p.Start());
        CHECK(WaitUntil([&] { return !ReadAll(dir + "/scoped.log").empty(); }, milliseconds(3000)));
        observed = static_cast<pid_t>(std::stol(ReadAll(dir + "/scoped.log")));
        CHECK(::kill(observed, 0) == 0);  // alive inside the scope
    }
    CHECK(WaitUntil([&] { return ::kill(observed, 0) != 0; }, milliseconds(3000)));  // gone after
}

void TestPortIsFree() {
    const int fd = ::socket(AF_INET, SOCK_STREAM, 0);
    int opt = 1;
    ::setsockopt(fd, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt));
    sockaddr_in addr{};
    addr.sin_family = AF_INET;
    addr.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
    addr.sin_port = 0;
    CHECK(::bind(fd, reinterpret_cast<sockaddr*>(&addr), sizeof(addr)) == 0);
    CHECK(::listen(fd, 4) == 0);
    socklen_t len = sizeof(addr);
    ::getsockname(fd, reinterpret_cast<sockaddr*>(&addr), &len);
    const int port = ntohs(addr.sin_port);

    CHECK(!harness::PortIsFree(port));  // live listener
    ::close(fd);
    CHECK(harness::PortIsFree(port));   // released
}

}  // namespace

int main() {
    const std::string dir = (std::filesystem::temp_directory_path() /
                             ("process_harness_test_" + std::to_string(::getpid())))
                                .string();
    std::filesystem::create_directories(dir);

    TestKillAndRestart(dir);
    TestTerminate(dir);
    TestLogsAndExitCodes(dir);
    TestDestructorStopsChild(dir);
    TestPortIsFree();

    std::error_code ec;
    std::filesystem::remove_all(dir, ec);
    if (g_failures == 0) {
        std::printf("process_harness_test: all checks passed\n");
        return 0;
    }
    std::fprintf(stderr, "process_harness_test: %d check(s) failed\n", g_failures);
    return 1;
}