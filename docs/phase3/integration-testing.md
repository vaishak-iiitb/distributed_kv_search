​

# Phase 3 integration testing: the 12-process test

`test/integration/multi_shard_test.cpp` runs the real server binaries as
child processes (3 `kv_metadata_server` + 2 shards x 3 `kv_shard_server`)
and drives them through the real client stack (`ClientRouter` ->
`RaftGroupClient` -> `LineClient`). It is the first test in the repo that
exercises metadata Raft, shard Raft, key ownership, the router and both
launchers together, on real sockets and real disk.

It complements, and does not replace, the faster tests that isolate one
layer: `test_client_router` (scripted fake servers), `shard_node_test`(real shard Raft, faked metadata), `metadata_group_test`.

## Running it

From the repo root:

```bash
cd build
cmake .. && make -j8 process_harness_test multi_shard_test
```

```bash
./process_harness_test
```

```bash
./multi_shard_test
```

The test finds `kv_metadata_server` and `kv_shard_server` in the directory
that contains the test executable. To point it at a different build
directory, pass that directory as the one argument; from inside `build/`this explicitly passes the current directory:

```bash
./multi_shard_test "$PWD"
```

Do not paste `#` comments after these commands into zsh: interactive zsh
does not treat `#` as a comment, so the text would be passed to the test as
arguments.

`make multi_shard_test` also builds `kv_metadata_server` and
`kv_shard_server` (CMake `add_dependencies`). Expect roughly 20-60 seconds.

Exit codes: `0` all checks passed, `1` a check failed, `2` environment
problem (binary missing, a port busy, work dir not creatable).

## Topology and ports

| What | Raft port | Client port |
| --- | --- | --- |
| metadata node i (i = 0..2) | 21000 + i | 21010 + i |
| shard s, node n | 21100 + 10s + n | 21200 + 10s + n |

Ports are constants so a stuck run is easy to diagnose. They avoid 5000 and
7000 (macOS AirPlay Receiver) and the 72xx/73xx/74xx/75xx ports used by the
manual `kv_admin` walkthrough. The test refuses to start (exit 2) if any of
them is busy, which almost always means a previous run leaked servers:

```bash
pkill kv_metadata_server; pkill kv_shard_server
```

Shard map: version 1, 2 shards x 3 nodes, 64 vnodes per shard.

## Phases and what each one proves

| Phase | Action | Proves |
| --- | --- | --- |
| 1 boot | start metadata group; start shard nodes BEFORE any map exists; publish map; wait until each shard commits a canary write | shard nodes wait for a map instead of failing; publish works against a real group; "ready" means a real commit, not an open port |
| 2 routing | 200 keys via `ClientRouter`; read back via router; read each key directly from its owner; ask the non-owning shard about a sample of keys | consistent-hash routing end to end; owner has the data; the other shard answers `WRONG_SHARD 1` with the right map version over the real wire |
| 3 shard crash | for EACH shard: find its current leader with the `LEADER` probe and SIGKILL it; read all 200 keys; write 10 new keys to that shard; restart the node. Then 5 rounds of: acknowledged Put, SIGKILL the leader IMMEDIATELY, read the key back | a shard survives losing its leader; committed data stays readable; a crashed node restarts from its persisted state; **a client that was told OK can read its own write after a leader change (flag #2)** |
| 4 metadata crash | find the metadata leader and SIGKILL it; a FRESH router refreshes the map and routes | the map stays available after losing the metadata leader |
| 5 shutdown | SIGTERM everything still running; every process must exit 0 | both launchers stop cleanly (ClientServer stopped before RaftNode) |

SIGKILL models a crash (no cleanup runs). SIGTERM models an orderly stop.

### Why the test kills the leader and not a fixed node

The launchers seed each node's election timer deterministically
(`1 + shard*64 + node` for shard nodes, `1 + id` for metadata nodes), so the
same node tends to win the first election in every run. An early version of
this test always killed node 0. On the author's machine the leaders turned
out to be shard 0 node 0, shard 1 node 2 and metadata node 1, so that
version only happened to kill a real leader for shard 0. The test now finds
the real leader via the `LEADER` probe (`FindLeader`) and kills that one,
and prints which node it killed.

### Why there are write-then-kill rounds (the flag #2 window)

Killing a leader seconds after the last write does not reach the risky
case. A `Put` is acknowledged once a majority has the entry, but followers
only learn it is COMMITTED from the leader's next `AppendEntries`, up to one
heartbeat (50 ms) later. After seconds of idle heartbeats every follower has
applied everything, so a new leader already has all the data.

The dangerous window is: acknowledged write, then the leader dies before the
next heartbeat. The new leader has the entry in its log (a majority had it,
and Raft's election restriction picks a leader that does) but may not have
applied it yet, because a new leader only advances its commit index after
committing an entry of its own term. If it serves a `Get` from local state
in that state, a client that was told OK cannot read its own write.

`PhaseWriteThenKill` hits that window on purpose: `Put` (acknowledged),
`Kill()` with no pause, then `Get` the same key, 5 rounds per shard. A
failure there is a server-side finding (no read barrier / leader no-op), not
a harness bug, and the test says so in its output.

## Reading a failure

Every process writes its stdout+stderr to `&lt;work dir&gt;/logs/<name>.log`. The
work dir is printed at start and is **kept when the test fails** (deleted
when it passes). Names: `meta0..2`, `shard<S>_node<N>`.

| Symptom | Likely meaning |
| --- | --- |
| exit 2, "port N is already in use" | leaked servers from an earlier run; `pkill` them |
| phase 1: "shard S never became writable" | a shard never elected a leader or never fetched the map; read `shardS_node*.log` |
| phase 1: nodes not running after 500 ms | a launcher crashed at startup; read its log |
| phase 2: foreign `Get` is not `kWrongShard` | key guard missing or ring/map mismatch between node and client |
| phase 3: `NOT_FOUND` for a key written before the crash, or "was ACKNOWLEDGED, then its leader ... was killed at once" | the killed node WAS the shard's leader, so the NEW leader served a read before applying a committed entry. This is known flag #2 (no read barrier / leader no-op on election), a server-side finding rather than a test bug. Decide flag #2 before Phase 4 |
| phase 3/4: "no node answered LEADER" | no leader within 10 s of a settled cluster; read the nodes' logs |
| phase 3: new leader election takes > 10 s | election timeouts or transport problem; read the surviving nodes' logs |
| phase 5: exit code != 0 | a launcher's shutdown path is broken (or it was force-killed after 5 s) |

## `process_harness.h`

Header-only, POSIX. Reused by Phase 4 chaos tests.

* `harness::Process(name, binary, args, log_path)`: `Start()` (fork/exec;
  stdout+stderr appended to the log, so restarts keep history), `Running()`,
  `Kill()` (SIGKILL = crash), `Terminate(timeout_ms)` (SIGTERM, then SIGKILL),
  `exit_code()`. The destructor terminates a still-running child, so a
  failing CHECK that returns early does not leak servers.
* `harness::PortIsFree(port)`, `harness::WaitUntil(predicate, timeout)`.

A test that crashes or is SIGKILLed cannot run destructors and WILL leak its
children; the next run's port pre-check reports it.

`process_harness_test` tests the harness alone (using only `/bin/sh` and
`/bin/sleep`): kill/restart, SIGTERM vs a child that ignores it, log capture
and append, exec failure (exit 127), destructor cleanup, port check.

## Deliberately not covered yet

* **Live map refresh.** Shard nodes read the map once at start. Publishing a
  version 2 map does not reach running nodes, so the router's refetch-on-
  `WRONG_SHARD` path is only tested against scripted servers
  (`test_client_router`). When `ShardNode` gains live refresh, extend this
  test with a map change under load.
* **Network partitions, message delay, clock skew.** Only process crashes.
  Partitions are Phase 4 chaos testing.
* **Linearizability.** `Get` is served by the node claiming leadership
  (flag #2); this test only checks that reads return what was written when
  nothing is racing.
* **Rebalancing / data migration.** Not implemented; a key that moves shards
  is simply not found on its new shard.

## How this test was validated when it was written

1. `process_harness_test`: compiled and run, all checks passed.
2. `multi_shard_test`: compiled, then run end to end against SANDBOX MOCK
   servers that speak the same line protocol with the same flags but no Raft
   (every node claims leadership; nodes of a group share a file-backed store).
   This validated the test's own logic, process management and exit codes.
   Mutation checks confirmed it fails when ownership is not enforced and
   when a server exits non-zero on SIGTERM, keeps logs on failure, leaks no
   processes, and returns 2 for a busy port or missing binary.
3. Mock update: the mocks were made leader-aware (the highest-id live node
   claims leadership, so killing it moves leadership) to confirm the test
   kills the actual leader (it killed node 2 in every group) and still
   passes, restarts it, and leaks no processes. A further mock variant that
   simulates flag #2 (a non-highest node answers NOT\_FOUND for the
   write-then-kill keys) made the test fail with the flag #2 explanation,
   stop at the first failure, keep its logs, and leave no processes.
4. Against the real binaries, Apple M1 MacBook Air (macOS). Runs are
   recorded in the table below. Rows before the write-then-kill rounds
   existed kill the leader (or node 0) seconds after the last write, so they
   do not exercise the flag #2 window.

| Date | Runs | Passed | Notes |
| --- | --- | --- | --- |
| 2026-10-13 | 1 | 1 | first real run, fixed-node kills |
| 2026-10-14 | 2 | 2 | default binary dir and explicit `$PWD` argument, fixed-node kills |
| 2026-10-14 | 10 | 10 | loop, fixed-node kills |
| 2026-10-14 | 11 | 11 | leader-targeted kills (shard 0 leader node 0, shard 1 leader node 2, metadata leader node 1), WITHOUT write-then-kill rounds |

​
   | 2026-10-14 | 11 | 0 | write-then-kill rounds FAILED every run: acknowledged write unreadable after immediate leader kill (flag #2 confirmed) |
   | 2026-10-18 | 10 | 10 | after flag #2 fix (leader no-op + read barrier) |
