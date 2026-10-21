// Regression test for bug D (match_index inflation): a leader must not commit
// an entry that no follower has.
//
// The original bug: on a successful AppendEntries reply the leader did
// `match = next; next += 1;` whether or not the request carried an entry. An
// EMPTY heartbeat therefore pushed match_index one past the leader's log. A
// heartbeat ack that was already in flight when a client write was appended
// then counted as "the follower has the new entry", and commit_index advanced
// although the entry had been sent to, and acknowledged by, nobody.
//
// The fix: AppendEntriesResponse carries match_index (the prefix the follower
// verified: prev_log_index + entries.size()), and the leader uses that value.
//
// The acks below are built the way a real follower builds them. An empty
// heartbeat with prev_log_index = P is acknowledged with match_index = P.
// (An earlier version of this test built acks as {term, success} only, which
// leaves match_index at 0 and so stopped modelling the scenario at all.)

#include <cstdio>

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

static Event Ack(bool success, LogIndex match_index, Term term = 1) {
    AppendEntriesResponse resp;
    resp.term = term;
    resp.success = success;
    resp.match_index = success ? match_index : 0;
    return Received<AppendEntriesResponse>{resp};
}

int main() {
    constexpr std::size_t kClusterSize = 3;

    RaftState s;
    s.self_id = 0;
    s.role = Role::kLeader;
    s.current_term = 1;
    s.log.append(1, "e1");  // index 1, already committed and applied everywhere
    s.commit_index = 1;
    s.last_applied = 1;

    // The first reply from each follower only initialises next_index/match_index.
    // (Both acknowledge the empty assert-leadership heartbeat with prev = 1.)
    handle(s, 1, Ack(true, 1), kClusterSize);
    handle(s, 2, Ack(true, 1), kClusterSize);

    // An EMPTY heartbeat (prev = 1, no entries) is acknowledged by follower 1.
    handle(s, 1, Ack(true, 1), kClusterSize);
    // Invariant: a follower's match_index can never exceed the leader's log length.
    CHECK(s.match_index[1] <= s.log.last_index());
    // And it is exactly what the follower verified.
    CHECK(s.match_index[1] == 1);

    // A later heartbeat (prev beyond the follower's log) is rejected.
    handle(s, 1, Ack(false, 0), kClusterSize);

    // A client write: the leader appends e2. It has been sent to no one.
    const ProposeResult proposed = propose(s, "e2");
    CHECK(proposed.accepted);
    CHECK(proposed.index == 2);

    // A heartbeat ack that was already in flight (sent before e2 existed,
    // prev = 1, no entries) arrives now.
    handle(s, 1, Ack(true, 1), kClusterSize);

    // Only the leader has e2: it must NOT be committed.
    CHECK(s.commit_index < proposed.index);
    CHECK(s.commit_index == 1);
    CHECK(s.match_index[1] == 1);

    // Liveness: the fix must not stop commits. When follower 1 really
    // acknowledges e2 (prev = 1, one entry => match_index 2), it commits.
    handle(s, 1, Ack(true, 2), kClusterSize);
    CHECK(s.match_index[1] == 2);
    CHECK(s.commit_index == 2);

    if (g_failures == 0) {
        std::printf("stale_ack_commit_test: all checks passed\n");
        return 0;
    }
    std::fprintf(stderr, "stale_ack_commit_test: %d check(s) failed\n", g_failures);
    return 1;
}