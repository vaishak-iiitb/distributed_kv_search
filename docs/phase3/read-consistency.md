# Read consistency after a leader change (flag #2) and a related commit bug

## Summary

| | Status |
| --- | --- |
| A. A NEW leader served reads before applying entries it had already acknowledged | **Found by `multi_shard_test`, reproduced, fixed** (leader no-op + read barrier) |
| B. A DEPOSED leader that has not noticed it was replaced can still answer reads from stale state | **Open.** Needs a ReadIndex-style leadership check |
| C. Followers answer a raw `GET` from local state | Unchanged, by design for now (`RaftGroupClient` never reads from a follower) |
| D. `match_index` inflation can let the leader commit an entry no follower has | **Open, proven at the state-machine level**, not yet seen in a live cluster |

## A. The bug that was found

`multi_shard_test`'s write-then-kill phase failed in 11 of 11 real runs:
a `Put` was acknowledged, the shard leader was SIGKILLed immediately, and
`Get` on the new leader returned `NOT_FOUND`.

Why: a `Put` is acknowledged once a majority has the entry and the leader
advances its commit index. Followers only learn the new commit index from the
leader's NEXT `AppendEntries`, up to one heartbeat (50 ms) later. If the
leader dies in that gap, the new leader has the entry in its log but its
commit index (and therefore its applied key-value state) is behind. In Raft a
leader can only advance its commit index by committing an entry of its OWN
term, so with no new client write it stays behind indefinitely (design doc
section 2.6). `ClientServer` served `GET` straight from the local KV engine.

Evidence it was "not applied" rather than "lost": after the failure, one more
successful write made `Get` find the old key.

## The fix (A)

1. **Leader no-op.** When a node becomes leader, `RaftNode` appends a no-op
   entry of the new term (`kNoopCommand`, `"NOOP"`, deliberately not a valid
   encoded `Command`; `PersistentRaftNode::ApplyCommitted` skips it). When it
   commits, every earlier entry is committed and applied.
2. **Read barrier.** `RaftNode::GetFromLeader` waits (up to the propose
   timeout) until the no-op is committed and applied, then reads. It returns
   `kNotLeader` if the node is not the leader, `kNotReady` if the barrier does
   not clear in time.
3. **`ClientServer` GET.** A leader answers via `GetFromLeader`(`TIMEOUT` if not ready). A non-leader keeps the old local read.
4. **`RaftGroupClient::Get`** treats `TIMEOUT` and `NOT_LEADER` replies to a
   `GET` as retryable (re-probe), like the write path.

The pure state machine (`state_machine.cpp`) is unchanged, so its unit tests
are unaffected. The no-op is added one layer up, in `RaftNode`.

Cost: one extra log entry per leader election; reads on a brand-new leader
wait about one or two heartbeats for the no-op to commit.

## What A does NOT guarantee (B, C)

* **B, deposed leader.** A leader that was partitioned away and has not heard
  a higher term still believes it is the leader and, its barrier long since
  passed, answers reads from its own (possibly stale) state. Fixing this needs
  the leader to confirm it still has a majority before answering (ReadIndex:
  record the commit index, get a heartbeat round acknowledged by a majority
  sent AFTER the read arrived, wait until applied >= that index). Reads then
  need a correlation between a heartbeat and its acknowledgement, which the
  current `AppendEntriesResponse` does not carry.
* **C, follower reads.** A raw `GET` to a follower returns its local state.
  Left as is so existing tests and tools keep working.

## D. `match_index` inflation (open)

`on_append_entries_response` handles a successful reply with
`match = next; next += 1;`, assuming every request carried exactly the entry
at `next`. An empty heartbeat (`next == last_index + 1`) also gets a success
reply, so `match_index[peer]` becomes `last_index + 1`: one past the
leader's log. Normally harmless because `maybe_advance_leader_commit` only
commits indexes whose entry has the current term, and an index beyond the log
has none. But once the leader appends an entry at exactly that index, a
heartbeat ack that was already in flight (sent before the entry existed)
makes the inflated match count as a replica, and the leader commits an entry
that is on the leader alone.

Reproduced deterministically against the real `state_machine.cpp` by
`test/unit/raft/stale_ack_commit_test.cpp` (known-failing; built but not
registered with ctest). The window in a live cluster is narrow (an ack in
flight while a write arrives, so roughly RTT / heartbeat interval of writes);
it has not been observed live. Consequence if it triggers and the leader then
dies: an acknowledged write is lost.

Likely fix: make the acknowledgement say how far the follower matched (add
`match_index` to `AppendEntriesResponse`, set to `prev_log_index +
entries.size()` on success) and have the leader use that value, or have the
leader remember what each request carried. Both touch the wire format in
`rpc.h/.cpp`.

## How this was verified

Done in a sandbox with the REAL `state_machine.cpp`, `persistent_node.cpp`,
`raft_node.cpp` and `client_server.cpp` and the real `RaftGroupClient`, but
stand-in `RaftTransport` (in-process queues), persistence, `KVEngine`,
`ThreadPool`, `rpc` and `command.cpp`:

* Baseline (your unmodified code): acknowledged write, leader killed at once,
  `Get` returned `NOT_FOUND` in 6 of 6 trials.
* After the fix: 12 of 12 trials returned the value. Also: 30 Put/Get/Delete
  round trips, follower raw `GET` unchanged, a quorum-less leader's `PUT`times out.
* `read_after_failover_test` (in the repo): fails 3 of 3 on the original code,
  passes on the fixed code.
* ThreadSanitizer: no reports in the new code. One pre-existing race in
  `ClientServer::Stop()` (`listen_fd_` written in `Stop()`, read in
  `AcceptLoop()`), unrelated to this change.

NOT verified by the sandbox: the real TCP transport, disk persistence, and
the real `multi_shard_test` run against the fixed binaries. See the run-record
table in `integration-testing.md`.
