// KNOWN-FAILING until fixed: a leader must not commit an entry that no
// follower has. Demonstrates the match_index inflation bug.
//
// In on_append_entries_response, a successful reply sets `match = next`
// whether or not the request carried an entry. An EMPTY heartbeat (sent when
// next == last_index + 1) therefore makes match_index point one past the
// leader's log. If a heartbeat ack that was already in flight arrives AFTER a
// client write was appended, that inflated match counts as "the follower has
// the new entry", and commit_index advances although the entry has not been
// sent to, or acknowledged by, anyone.
//
// This test is built (target `stale_ack_commit_test`) but deliberately NOT
// registered with ctest yet. Register it with add_test() once the bug is fixed.

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

static Event Ack(bool success, Term term = 1) {
    return Received<AppendEntriesResponse>{AppendEntriesResponse{term, success}};
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
    handle(s, 1, Ack(true), kClusterSize);
    handle(s, 2, Ack(true), kClusterSize);

    // An EMPTY heartbeat (prev = 1, no entries) is acknowledged by follower 1.
    handle(s, 1, Ack(true), kClusterSize);
    // Invariant: a follower's match_index can never exceed the leader's log length.
    CHECK(s.match_index[1] <= s.log.last_index());

    // The following heartbeat (prev beyond the follower's log) is rejected.
    handle(s, 1, Ack(false), kClusterSize);

    // A client write: the leader appends e2. It has been sent to no one.
    const ProposeResult proposed = propose(s, "e2");
    CHECK(proposed.accepted);

    // A heartbeat ack that was already in flight (sent before e2 existed,
    // prev = 1, no entries) arrives now.
    handle(s, 1, Ack(true), kClusterSize);

    // Only the leader has e2: it must NOT be committed.
    CHECK(s.commit_index < proposed.index);

    if (g_failures == 0) {
        std::printf("stale_ack_commit_test: all checks passed\n");
        return 0;
    }
    std::fprintf(stderr, "stale_ack_commit_test: %d check(s) failed (known bug: match_index inflation)\n",
                 g_failures);
    return 1;
}