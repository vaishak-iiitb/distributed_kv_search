// Tests for the match_index field of AppendEntriesResponse (bug D fix).
//
//  1. Follower side: what a follower reports as match_index.
//  2. Leader side: how the leader uses it (clamp, monotonic, next only moves
//     forward on success).
//  3. A randomized property test over FakeCluster: with writes and empty
//     heartbeats interleaved and messages delivered in random order, the
//     leader's match_index must never exceed its own log, must describe a
//     prefix the follower really holds, and commit_index must always be on a
//     real majority of logs.
//
// What this does NOT cover: real TCP, real timers, crashes and restarts.

#include <cstdio>
#include <random>
#include <string>

#include "raft/fake_cluster.h"
#include "raft/state_machine.h"

using namespace raft;

static int g_failures = 0;
#define CHECK(cond)                                                                       \
    do {                                                                                  \
        if (!(cond)) {                                                                    \
            std::fprintf(stderr, "%s:%d: CHECK failed: %s\n", __FILE__, __LINE__, #cond); \
            ++g_failures;                                                                 \
        }                                                                                 \
    } while (0)

namespace {

constexpr std::size_t kCluster3 = 3;

// ---------------------------------------------------------------------------
// 1. Follower side
// ---------------------------------------------------------------------------

RaftState MakeFollower(std::size_t log_len, Term entry_term = 1) {
    RaftState f;
    f.self_id = 1;
    f.role = Role::kFollower;
    f.current_term = 2;
    for (std::size_t i = 0; i < log_len; ++i) {
        f.log.append(entry_term, "f" + std::to_string(i + 1));
    }
    return f;
}

AppendEntriesRequest MakeAppend(Term term, LogIndex prev, Term prev_term,
                                std::vector<LogEntry> entries) {
    AppendEntriesRequest req;
    req.term = term;
    req.leader_id = 0;
    req.prev_log_index = prev;
    req.prev_log_term = prev_term;
    req.entries = std::move(entries);
    req.leader_commit = 0;
    return req;
}

// Runs the request on the follower and returns its AppendEntriesResponse.
AppendEntriesResponse Reply(RaftState& f, const AppendEntriesRequest& req) {
    const HandleResult r = handle(f, 0, Received<AppendEntriesRequest>{req}, kCluster3);
    CHECK(r.messages.size() == 1);
    if (r.messages.size() != 1) return AppendEntriesResponse{};
    const auto* resp = std::get_if<AppendEntriesResponse>(&r.messages[0].message);
    CHECK(resp != nullptr);
    return resp ? *resp : AppendEntriesResponse{};
}

void test_follower_heartbeat_on_empty_log() {
    RaftState f = MakeFollower(0);
    const AppendEntriesResponse resp = Reply(f, MakeAppend(2, 0, 0, {}));
    CHECK(resp.success);
    CHECK(resp.match_index == 0);
}

void test_follower_heartbeat_reports_prev_not_prev_plus_one() {
    RaftState f = MakeFollower(2);
    const AppendEntriesResponse resp = Reply(f, MakeAppend(2, 2, 1, {}));
    CHECK(resp.success);
    CHECK(resp.match_index == 2);  // the old leader logic would have said 3
}

void test_follower_one_entry_reports_prev_plus_one() {
    RaftState f = MakeFollower(2);
    const AppendEntriesResponse resp = Reply(f, MakeAppend(2, 2, 1, {{2, "x"}}));
    CHECK(resp.success);
    CHECK(resp.match_index == 3);
    CHECK(f.log.last_index() == 3);
}

void test_follower_two_entries_from_empty() {
    RaftState f = MakeFollower(0);
    const AppendEntriesResponse resp = Reply(f, MakeAppend(2, 0, 0, {{2, "a"}, {2, "b"}}));
    CHECK(resp.success);
    CHECK(resp.match_index == 2);
}

// The follower holds 5 entries; the request only verifies through index 3.
// match_index must be what the REQUEST verified (3), not the follower's own
// last index (5): entries 4 and 5 are unverified and may differ from the
// leader's log.
void test_follower_longer_log_reports_verified_prefix_not_own_last_index() {
    RaftState f = MakeFollower(5);
    // Entry 3 in the request is identical to the follower's entry 3 (term 1,
    // "f3"), so nothing is truncated.
    const AppendEntriesResponse resp = Reply(f, MakeAppend(2, 2, 1, {{1, "f3"}}));
    CHECK(resp.success);
    CHECK(f.log.last_index() == 5);
    CHECK(resp.match_index == 3);
}

void test_follower_failure_replies_carry_zero_match_index() {
    {  // prev_log_index beyond the follower's log
        RaftState f = MakeFollower(2);
        const AppendEntriesResponse resp = Reply(f, MakeAppend(2, 10, 1, {}));
        CHECK(!resp.success);
        CHECK(resp.match_index == 0);
    }
    {  // prev entry exists but its term differs
        RaftState f = MakeFollower(2);
        const AppendEntriesResponse resp = Reply(f, MakeAppend(2, 2, 7, {}));
        CHECK(!resp.success);
        CHECK(resp.match_index == 0);
    }
    {  // request from a stale term
        RaftState f = MakeFollower(2);
        const AppendEntriesResponse resp = Reply(f, MakeAppend(1, 2, 1, {}));
        CHECK(!resp.success);
        CHECK(resp.match_index == 0);
    }
}

// ---------------------------------------------------------------------------
// 2. Leader side
// ---------------------------------------------------------------------------

RaftState MakeLeader(std::size_t log_len) {
    RaftState s;
    s.self_id = 0;
    s.role = Role::kLeader;
    s.current_term = 1;
    for (std::size_t i = 0; i < log_len; ++i) s.log.append(1, "l" + std::to_string(i + 1));
    return s;
}

Event Ack(bool success, LogIndex match_index, Term term = 1) {
    AppendEntriesResponse resp;
    resp.term = term;
    resp.success = success;
    resp.match_index = match_index;
    return Received<AppendEntriesResponse>{resp};
}

void test_leader_clamps_match_to_own_log() {
    RaftState s = MakeLeader(6);
    s.next_index[1] = 5;
    s.match_index[1] = 1;
    s.next_index[2] = 7;
    s.match_index[2] = 0;
    handle(s, 1, Ack(true, 99), kCluster3);
    CHECK(s.match_index[1] == 6);  // clamped to last_index, not 99
}

void test_leader_match_never_moves_backwards() {
    RaftState s = MakeLeader(6);
    s.next_index[1] = 5;
    s.match_index[1] = 1;
    s.next_index[2] = 7;
    s.match_index[2] = 0;
    handle(s, 1, Ack(true, 4), kCluster3);
    CHECK(s.match_index[1] == 4);
    handle(s, 1, Ack(true, 2), kCluster3);  // a late, older ack
    CHECK(s.match_index[1] == 4);
}

void test_leader_next_only_moves_forward_on_success() {
    RaftState s = MakeLeader(6);
    s.next_index[1] = 5;
    s.match_index[1] = 1;
    s.next_index[2] = 7;
    s.match_index[2] = 0;
    handle(s, 1, Ack(true, 2), kCluster3);
    CHECK(s.match_index[1] == 2);
    CHECK(s.next_index[1] == 5);  // an ack for 2 must not drag next back to 3
}

void test_leader_next_catches_up_to_match_plus_one() {
    RaftState s = MakeLeader(6);
    s.next_index[1] = 2;  // dragged down by stale failures
    s.match_index[1] = 4;
    s.next_index[2] = 7;
    s.match_index[2] = 0;
    handle(s, 1, Ack(true, 4), kCluster3);
    CHECK(s.match_index[1] == 4);
    CHECK(s.next_index[1] == 5);
}

void test_leader_ignores_ack_from_older_term() {
    RaftState s = MakeLeader(6);
    s.current_term = 3;
    s.next_index[1] = 5;
    s.match_index[1] = 1;
    s.next_index[2] = 7;
    s.match_index[2] = 0;
    handle(s, 1, Ack(true, 6, /*term=*/2), kCluster3);
    CHECK(s.match_index[1] == 1);
    CHECK(s.next_index[1] == 5);
}

// ---------------------------------------------------------------------------
// 3. Randomized property test over FakeCluster
// ---------------------------------------------------------------------------

bool prefix_matches(const RaftState& leader, const RaftState& follower, LogIndex upto) {
    for (LogIndex i = 1; i <= upto; ++i) {
        const auto a = leader.log.at(i);
        const auto b = follower.log.at(i);
        if (!a || !b) return false;
        if (a->term != b->term || a->command != b->command) return false;
    }
    return true;
}

// Returns an empty string when every invariant holds, else a description.
std::string CheckInvariants(const FakeCluster& cluster, NodeId leader_id) {
    const RaftState& leader = cluster.node(leader_id);
    if (leader.role != Role::kLeader) return "leader lost its role";

    for (const auto& [peer, match] : leader.match_index) {
        if (match > leader.log.last_index()) {
            return "match_index[" + std::to_string(peer) + "]=" + std::to_string(match) +
                   " exceeds leader last_index=" + std::to_string(leader.log.last_index());
        }
        if (!prefix_matches(leader, cluster.node(peer), match)) {
            return "match_index[" + std::to_string(peer) + "]=" + std::to_string(match) +
                   " claims a prefix the follower does not hold";
        }
    }

    if (leader.commit_index > 0) {
        std::size_t holders = 0;
        for (std::size_t i = 0; i < cluster.node_count(); ++i) {
            const RaftState& n = cluster.node(static_cast<NodeId>(i));
            const auto a = leader.log.at(leader.commit_index);
            const auto b = n.log.at(leader.commit_index);
            if (a && b && a->term == b->term && a->command == b->command) ++holders;
        }
        if (holders < cluster.node_count() / 2 + 1) {
            return "commit_index=" + std::to_string(leader.commit_index) +
                   " is held by only " + std::to_string(holders) + " node(s)";
        }
    }
    return "";
}

// Returns true if the run completed with no violation.
bool RunRandomSchedule(std::size_t nodes, std::uint64_t seed, std::string* why) {
    FakeCluster cluster(nodes, seed);
    cluster.inject(0, 0, ElectionTimeout{});
    cluster.drain_random(10000);
    if (cluster.node(0).role != Role::kLeader) {
        *why = "node 0 did not become leader";
        return false;
    }

    std::mt19937 pick(seed * 7919u + 13u);
    int next_cmd = 0;
    for (int step = 0; step < 120; ++step) {
        const int action = static_cast<int>(pick() % 10);
        if (action < 3) {
            cluster.propose(0, "c" + std::to_string(next_cmd++));
        } else if (action < 6) {
            // An empty heartbeat built from the leader's CURRENT last entry.
            // It may be delivered long after more entries were appended.
            const RaftState& l = cluster.node(0);
            AppendEntriesRequest hb;
            hb.term = l.current_term;
            hb.leader_id = 0;
            hb.prev_log_index = l.log.last_index();
            hb.prev_log_term = l.log.last_term();
            hb.leader_commit = l.commit_index;
            const NodeId target = static_cast<NodeId>(1 + pick() % (nodes - 1));
            cluster.inject(target, 0, Received<AppendEntriesRequest>{hb});
        } else {
            cluster.tick_random();
        }
        const std::string problem = CheckInvariants(cluster, 0);
        if (!problem.empty()) {
            *why = "step " + std::to_string(step) + ": " + problem;
            return false;
        }
    }
    cluster.drain_random(10000);
    const std::string problem = CheckInvariants(cluster, 0);
    if (!problem.empty()) {
        *why = "after drain: " + problem;
        return false;
    }
    return true;
}

void test_random_schedules_keep_match_and_commit_sound() {
    int reported = 0;
    for (std::size_t nodes : {3u, 5u}) {
        // 40 seeds per cluster size is about 2 s in a Debug build. On the
        // pre-fix leader logic this fails on the first or second seed.
        for (std::uint64_t seed = 1; seed <= 40; ++seed) {
            std::string why;
            if (!RunRandomSchedule(nodes, seed, &why)) {
                CHECK(false);
                if (reported++ < 3) {
                    std::fprintf(stderr, "  property violated: nodes=%zu seed=%llu: %s\n", nodes,
                                 static_cast<unsigned long long>(seed), why.c_str());
                }
            }
        }
    }
}

}  // namespace

int main() {
    test_follower_heartbeat_on_empty_log();
    test_follower_heartbeat_reports_prev_not_prev_plus_one();
    test_follower_one_entry_reports_prev_plus_one();
    test_follower_two_entries_from_empty();
    test_follower_longer_log_reports_verified_prefix_not_own_last_index();
    test_follower_failure_replies_carry_zero_match_index();

    test_leader_clamps_match_to_own_log();
    test_leader_match_never_moves_backwards();
    test_leader_next_only_moves_forward_on_success();
    test_leader_next_catches_up_to_match_plus_one();
    test_leader_ignores_ack_from_older_term();

    test_random_schedules_keep_match_and_commit_sound();

    if (g_failures == 0) {
        std::printf("ack_match_index_test: all checks passed\n");
        return 0;
    }
    std::fprintf(stderr, "ack_match_index_test: %d check(s) failed\n", g_failures);
    return 1;
}