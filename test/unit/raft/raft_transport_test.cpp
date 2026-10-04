#include "raft/raft_transport.h"

#include <chrono>
#include <condition_variable>
#include <cstdio>
#include <mutex>
#include <set>
#include <thread>
#include <vector>

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

// Every test here is genuinely asynchronous — the first time in this
// project delivery doesn't happen synchronously inside the call that
// triggered it. This sink is how a test waits for delivery instead of
// just inspecting state immediately afterward.
class ReceivedSink {
public:
    void OnMessage(raft::NodeId from, const raft::Message& msg) {
        std::lock_guard<std::mutex> lock(mutex_);
        received_.emplace_back(from, msg);
        cv_.notify_all();
    }

    bool WaitForCount(std::size_t count, std::chrono::milliseconds timeout) {
        std::unique_lock<std::mutex> lock(mutex_);
        return cv_.wait_for(lock, timeout, [&] { return received_.size() >= count; });
    }

    std::vector<std::pair<raft::NodeId, raft::Message>> Snapshot() {
        std::lock_guard<std::mutex> lock(mutex_);
        return received_;
    }

private:
    std::mutex mutex_;
    std::condition_variable cv_;
    std::vector<std::pair<raft::NodeId, raft::Message>> received_;
};

constexpr auto kTimeout = std::chrono::milliseconds(2000);

void test_basic_send_and_receive() {
    ReceivedSink sink;
    raft::RaftTransport receiver(
        1, 17001, {}, [&](raft::NodeId from, const raft::Message& m) {
            sink.OnMessage(from, m);
        });
    CHECK(receiver.Start());

    raft::RaftTransport sender(0, 17002, {{1, raft::PeerAddress{"127.0.0.1", 17001}}},
                               [](raft::NodeId, const raft::Message&) {});
    CHECK(sender.Start());

    raft::RequestVoteRequest req;
    req.term = 5;
    req.candidate_id = 0;
    req.last_log_index = 3;
    req.last_log_term = 2;
    sender.Send(1, req);

    CHECK(sink.WaitForCount(1, kTimeout));
    const auto received = sink.Snapshot();
    CHECK(received.size() == 1);
    CHECK(received[0].first == 0);  // identified via the handshake
    const auto* got = std::get_if<raft::RequestVoteRequest>(&received[0].second);
    CHECK(got != nullptr);
    if (got) {
        CHECK(got->term == 5);
        CHECK(got->last_log_index == 3);
    }
}

// Five independently-enqueued Send() calls to the same peer. Each
// ThreadPool task acquires outbound_mutex_ before writing (design notes
// §3.5), so no frame is ever corrupted by interleaved writes — but which
// task acquires the lock FIRST is an OS scheduling decision, not
// something tied to Send() call order. This test deliberately does NOT
// assert arrival order: Raft's own protocol is explicitly designed to
// tolerate reordering (design doc §4.2), so RaftTransport promising an
// ordering guarantee here would be solving a problem Raft doesn't have.
// What's actually required — and checked here — is that all five
// distinct, uncorrupted messages arrive.
void test_multiple_messages_all_arrive_uncorrupted() {
    ReceivedSink sink;
    raft::RaftTransport receiver(1, 17011, {}, [&](raft::NodeId from, const raft::Message& m) {
        sink.OnMessage(from, m);
    });
    CHECK(receiver.Start());

    raft::RaftTransport sender(0, 17012, {{1, raft::PeerAddress{"127.0.0.1", 17011}}},
                               [](raft::NodeId, const raft::Message&) {});
    CHECK(sender.Start());

    for (raft::Term t = 1; t <= 5; ++t) {
        raft::RequestVoteRequest req;
        req.term = t;
        sender.Send(1, req);
    }

    CHECK(sink.WaitForCount(5, kTimeout));
    const auto received = sink.Snapshot();
    CHECK(received.size() == 5);

    std::set<raft::Term> seen_terms;
    for (const auto& [from, msg] : received) {
        CHECK(from == 0);
        const auto* got = std::get_if<raft::RequestVoteRequest>(&msg);
        CHECK(got != nullptr);  // uncorrupted — decode succeeded and typed correctly
        if (got) seen_terms.insert(got->term);
    }
    // All five distinct terms present, regardless of arrival order.
    CHECK(seen_terms == std::set<raft::Term>({1, 2, 3, 4, 5}));
}

// Unreachable peer: a port nobody is listening on. Must not crash or hang.
void test_send_to_unreachable_peer_does_not_crash() {
    raft::RaftTransport sender(0, 17022, {{1, raft::PeerAddress{"127.0.0.1", 17099}}},
                               [](raft::NodeId, const raft::Message&) {});
    CHECK(sender.Start());

    raft::RequestVoteRequest req;
    req.term = 1;
    sender.Send(1, req);

    std::this_thread::sleep_for(std::chrono::milliseconds(200));
    // Nothing to assert beyond "the process is still here."
}

// A peer id not present in the `peers` map at all.
void test_send_to_unknown_peer_id_does_not_crash() {
    raft::RaftTransport sender(0, 17032, /*peers=*/{}, [](raft::NodeId, const raft::Message&) {});
    CHECK(sender.Start());

    raft::RequestVoteRequest req;
    req.term = 1;
    sender.Send(42, req);

    std::this_thread::sleep_for(std::chrono::milliseconds(200));
}

// Specifically a Response type (no sender id in its own payload — Step
// 2b design notes §5.3/§6) — proves `from` comes from the handshake,
// never from the message content itself.
void test_response_type_identified_via_handshake_not_payload() {
    ReceivedSink sink;
    raft::RaftTransport receiver(1, 17041, {}, [&](raft::NodeId from, const raft::Message& m) {
        sink.OnMessage(from, m);
    });
    CHECK(receiver.Start());

    raft::RaftTransport sender(0, 17042, {{1, raft::PeerAddress{"127.0.0.1", 17041}}},
                               [](raft::NodeId, const raft::Message&) {});
    CHECK(sender.Start());

    raft::RequestVoteResponse resp;
    resp.term = 3;
    resp.vote_granted = true;
    sender.Send(1, resp);

    CHECK(sink.WaitForCount(1, kTimeout));
    CHECK(sink.Snapshot()[0].first == 0);
}

// Each direction is its own independent connection, not one shared
// bidirectional socket.
void test_bidirectional_independent_connections() {
    ReceivedSink sink_a, sink_b;
    raft::RaftTransport node_a(
        0, 17051, {{1, raft::PeerAddress{"127.0.0.1", 17052}}},
        [&](raft::NodeId from, const raft::Message& m) { sink_a.OnMessage(from, m); });
    raft::RaftTransport node_b(
        1, 17052, {{0, raft::PeerAddress{"127.0.0.1", 17051}}},
        [&](raft::NodeId from, const raft::Message& m) { sink_b.OnMessage(from, m); });
    CHECK(node_a.Start());
    CHECK(node_b.Start());

    raft::RequestVoteRequest req_from_a;
    req_from_a.term = 7;
    node_a.Send(1, req_from_a);

    raft::RequestVoteRequest req_from_b;
    req_from_b.term = 8;
    node_b.Send(0, req_from_b);

    CHECK(sink_b.WaitForCount(1, kTimeout));
    CHECK(sink_a.WaitForCount(1, kTimeout));
    CHECK(sink_b.Snapshot()[0].first == 0);
    CHECK(sink_a.Snapshot()[0].first == 1);
}

// Repeated Stop()/destruction with an active connection must not hang —
// the exact failure mode design notes §3.4 walks through.
void test_stop_with_active_connection_does_not_hang() {
    ReceivedSink sink;
    auto receiver = std::make_unique<raft::RaftTransport>(
        1, 17061, std::map<raft::NodeId, raft::PeerAddress>{},
        [&](raft::NodeId from, const raft::Message& m) { sink.OnMessage(from, m); });
    CHECK(receiver->Start());

    raft::RaftTransport sender(0, 17062, {{1, raft::PeerAddress{"127.0.0.1", 17061}}},
                               [](raft::NodeId, const raft::Message&) {});
    CHECK(sender.Start());

    raft::RequestVoteRequest req;
    req.term = 1;
    sender.Send(1, req);
    CHECK(sink.WaitForCount(1, kTimeout));  // connection is now open and idle

    // If Stop() (called via the destructor here) doesn't unblock the
    // inbound read, this line never returns and the test hangs/times
    // out rather than failing cleanly — that itself would be the bug.
    receiver.reset();
}

}  // namespace

int main() {
    test_basic_send_and_receive();
    test_multiple_messages_all_arrive_uncorrupted();
    test_send_to_unreachable_peer_does_not_crash();
    test_send_to_unknown_peer_id_does_not_crash();
    test_response_type_identified_via_handshake_not_payload();
    test_bidirectional_independent_connections();
    test_stop_with_active_connection_does_not_hang();

    if (g_failures == 0) {
        std::printf("raft_transport_test: all checks passed\n");
        return 0;
    }
    std::fprintf(stderr, "raft_transport_test: %d check(s) failed\n", g_failures);
    return 1;
}