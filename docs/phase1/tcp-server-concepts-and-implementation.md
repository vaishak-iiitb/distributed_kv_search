# TCP Server — Concepts & Full Implementation (Phase 1, Step 7 — Completes Phase 1)

This is the last Phase 1 piece from the build order (§1.6, step 7):
*"Wire it behind a TCP server exposing GET/PUT/DELETE."* Everything
built so far — skip list, WAL, memtable, bloom filter, SSTable,
compaction — has been tested in isolation. This step composes all of
it into one real, running, crash-recoverable, network-accessible KV
store. Once this passes, Phase 1's actual milestone becomes something
you can literally do: *kill -9 the running server mid-write, restart
it, and confirm nothing acknowledged was lost.*

Two new pieces, in order: **`KVEngine`** (composes Memtable + SSTables
+ Compactor, with startup recovery) and **`TCPServer`** (a thread-pool
socket server that parses a simple text protocol and calls into
`KVEngine`). Trace through what happens to one `PUT`, end to end, from
socket bytes to disk and back, before running anything — that full
path is the thing worth being able to explain.

**Note on repo structure**: `src/storage/kv_engine.h/.cpp` and
`src/server_main.cpp` aren't in the original scaffold — they're new
files this step adds. Everything else (`src/net/thread_pool.*`,
`src/net/tcp_server.*`, `tools/cli.cpp`, `test/integration/`) already
had empty placeholders waiting to be filled in.

---

## 1. What `KVEngine` Actually Composes

Up to now, `Memtable` and `SSTable` have been separate, independently
tested pieces. Nothing yet knows how to:
- Hold **one active memtable** plus **a growing list of immutable
  SSTables** at the same time.
- Check them **in the right order** on a `Get` (memtable first, then
  SSTables newest-to-oldest — exactly design doc §1.4's read path).
- **Flush** the memtable to a new SSTable once it's full, and start a
  fresh one (§1.3's write path, step 4).
- **Trigger compaction** once too many SSTables have piled up.
- **Recover** all of this correctly from whatever's sitting on disk
  after a crash or restart.

`KVEngine` is that missing composition layer — it owns all the moving
parts and enforces the correct ordering between them.

## 2. Why the Read Order Matters (Recap, Now Composed for Real)

A `Get` must check, in this exact order, stopping at the first
definitive answer:

1. **Active memtable** — `kFound` → return the value. `kDeleted` →
   return "not found" *immediately*, without checking anything older
   (the tombstone's whole job, from the memtable doc, is to shadow
   stale data below it). `kNotPresent` → keep going.
2. **SSTables, newest to oldest** — same three-way logic per file. The
   first file that gives a definitive answer (`kFound` or `kDeleted`)
   wins; `kNotPresent` means keep checking older files.
3. Exhausted every SSTable with no match → genuinely not present.

This is why `KVEngine` needs to track SSTable files in a way that
preserves *age order* — iterating newest-to-oldest is the only way a
newer `Put` or `Delete` correctly shadows an older one that happens to
still be sitting in an unflushed/uncompacted file.

## 3. Crash Recovery, For Real This Time

`Memtable::RecoverFromWal()` and `Wal`'s replay logic have existed
since earlier steps, but nothing has actually *used* them on startup
yet — every test so far constructed fresh objects. `KVEngine`'s
constructor is where that finally happens:

1. Scan the data directory for existing `wal_*.log` and `sst_*.sst`
   files left over from a previous run.
2. If a WAL file exists, construct a `Memtable` pointed at it and call
   `RecoverFromWal()` — this replays every durably-written record back
   into a fresh skip list, exactly reconstructing pre-crash memtable
   state.
3. Sort discovered SSTable files by their numeric ID (assigned
   monotonically at creation time, whether from a flush or from
   compaction) — this ID ordering *is* the oldest-to-newest ordering
   the read path and compaction both depend on.

This is the mechanism that makes Phase 1's actual milestone true. It's
worth restarting the server yourself after writing some data and
confirming this works, not just trusting the automated test.

## 4. Why a Thread Pool, Not One Thread Per Connection

A naive TCP server spawns a new OS thread for every incoming
connection. That works until you have thousands of concurrent clients
— OS threads aren't free (stack allocation, scheduler overhead), and
an unbounded number of them is a real resource-exhaustion risk. A
**thread pool** fixes this by capping concurrency at a known number of
worker threads, each pulled from a shared task queue as work arrives —
connections get *handled* concurrently up to the pool size, without
the server's thread count growing unboundedly under load.

This implementation's pool: a fixed number of worker threads block on
a condition variable until a task is enqueued, pop it, run it, and go
back to waiting. `TCPServer` enqueues one task per accepted
connection — that task owns the connection for its entire lifetime
(reading and responding to as many commands as the client sends over
that one socket), not one task per individual command.

## 5. Why `KVEngine` Needs a Mutex

Multiple thread-pool workers can call `Put`/`Get`/`Delete` on the same
`KVEngine` instance concurrently (one client per worker, all sharing
one engine). Without synchronization, two threads could, for example,
both observe `memtable_->IsFull() == false`, both proceed to insert,
and race on the underlying skip list — which, as the skip list doc
noted at the time, was never built to be thread-safe.

This implementation uses a single coarse-grained mutex around the
entire engine — every `Put`, `Get`, and `Delete` takes the same lock
for its whole duration. This is a deliberate simplicity-over-throughput
choice, correct and matching this project's Phase 1 priority (per
§1.6: prove each layer correct before optimizing it). Real systems use
much finer-grained concurrency (per-shard locks, lock-free structures)
— worth naming as the obvious next optimization if asked, without
needing to have built it yet.

## 6. The Wire Protocol — A Deliberate Simplification

The WAL used a binary, length-prefixed format because it's an internal
hot path where parsing overhead and byte-exactness matter (WAL doc,
section 3). The client-facing protocol here is the opposite: a plain
text, line-based format —

```
PUT <key> <value>\n   -> OK\n              (or ERROR ...\n)
GET <key>\n           -> VALUE <value>\n   (or NOT_FOUND\n)
DELETE <key>\n        -> OK\n              (or ERROR ...\n)
```

This is simple enough to test by hand with `nc localhost 9090` and
typing commands directly — genuinely useful for manual sanity-checking
during development. The real cost: keys and values can't contain
spaces or newlines in this scheme, which a production protocol
(Redis's RESP, for instance) solves with proper length-prefixed
framing, same as the WAL does internally. Worth naming as a known,
deliberate limitation rather than an oversight — and worth noticing
that swapping this protocol out later wouldn't touch `KVEngine` at
all, since parsing lives entirely in `TCPServer`. That separation is
intentional: storage logic and wire format are different concerns,
kept in different files, and Phase 2's Raft RPCs will get a real
binary format when the stakes are higher (client requests can stay
simple text while node-to-node Raft RPCs need real binary framing —
different traffic, different needs).

---

## 7. Build Setup

Full `CMakeLists.txt` — replace wholesale (adds `Threads`, the new
`net` library, `kv_engine.cpp`, the integration test, the server
binary, and the CLI tool):

```cmake
cmake_minimum_required(VERSION 3.16)
project(distributed_kv_store CXX)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -Wall -Wextra -g")

find_package(Threads REQUIRED)
enable_testing()

add_library(storage
    src/storage/skiplist.cpp
    src/storage/wal.cpp
    src/storage/memtable.cpp
    src/storage/bloom_filter.cpp
    src/storage/sstable.cpp
    src/storage/compaction.cpp
    src/storage/kv_engine.cpp
)
target_include_directories(storage PUBLIC src)
target_link_libraries(storage PUBLIC Threads::Threads)

add_library(net
    src/net/thread_pool.cpp
    src/net/tcp_server.cpp
)
target_include_directories(net PUBLIC src)
target_link_libraries(net PUBLIC storage Threads::Threads)

# --- storage unit tests ---
add_executable(test_skiplist test/unit/storage/test_skiplist.cpp)
target_link_libraries(test_skiplist PRIVATE storage)
add_test(NAME test_skiplist COMMAND test_skiplist)

add_executable(test_wal test/unit/storage/test_wal.cpp)
target_link_libraries(test_wal PRIVATE storage)
add_test(NAME test_wal COMMAND test_wal)

add_executable(test_memtable test/unit/storage/test_memtable.cpp)
target_link_libraries(test_memtable PRIVATE storage)
add_test(NAME test_memtable COMMAND test_memtable)

add_executable(test_bloom_filter test/unit/storage/test_bloom_filter.cpp)
target_link_libraries(test_bloom_filter PRIVATE storage)
add_test(NAME test_bloom_filter COMMAND test_bloom_filter)

add_executable(test_sstable test/unit/storage/test_sstable.cpp)
target_link_libraries(test_sstable PRIVATE storage)
add_test(NAME test_sstable COMMAND test_sstable)

add_executable(test_compaction test/unit/storage/test_compaction.cpp)
target_link_libraries(test_compaction PRIVATE storage)
add_test(NAME test_compaction COMMAND test_compaction)

# --- integration test (real sockets, real disk) ---
add_executable(test_tcp_server test/integration/tcp_server_test.cpp)
target_link_libraries(test_tcp_server PRIVATE net storage)
add_test(NAME test_tcp_server COMMAND test_tcp_server)

# --- server binary + manual CLI client ---
add_executable(kv_server src/server_main.cpp)
target_link_libraries(kv_server PRIVATE net storage)

add_executable(cli tools/cli.cpp)
```

Build and run:

```bash
rm -rf build
cmake -B build
cmake --build build
./build/test_tcp_server
```

---

## 8. Full Code

### `src/net/thread_pool.h`

```cpp
#pragma once

#include <condition_variable>
#include <functional>
#include <mutex>
#include <queue>
#include <thread>
#include <vector>

// Fixed-size pool of worker threads pulling tasks from a shared queue.
// Caps concurrency at a known number of OS threads instead of spawning
// one per unit of work — see section 4 of the accompanying doc.
class ThreadPool {
 public:
  explicit ThreadPool(size_t num_threads);
  ~ThreadPool();

  ThreadPool(const ThreadPool&) = delete;
  ThreadPool& operator=(const ThreadPool&) = delete;

  void Enqueue(std::function<void()> task);

 private:
  void WorkerLoop();

  std::vector<std::thread> workers_;
  std::queue<std::function<void()>> tasks_;
  std::mutex mutex_;
  std::condition_variable cv_;
  bool stop_;
};
```

### `src/net/thread_pool.cpp`

```cpp
#include "net/thread_pool.h"

ThreadPool::ThreadPool(size_t num_threads) : stop_(false) {
  for (size_t i = 0; i < num_threads; i++) {
    workers_.emplace_back(&ThreadPool::WorkerLoop, this);
  }
}

ThreadPool::~ThreadPool() {
  {
    std::lock_guard<std::mutex> lock(mutex_);
    stop_ = true;
  }
  cv_.notify_all();
  for (auto& worker : workers_) {
    if (worker.joinable()) worker.join();
  }
}

void ThreadPool::Enqueue(std::function<void()> task) {
  {
    std::lock_guard<std::mutex> lock(mutex_);
    tasks_.push(std::move(task));
  }
  cv_.notify_one();
}

void ThreadPool::WorkerLoop() {
  while (true) {
    std::function<void()> task;
    {
      std::unique_lock<std::mutex> lock(mutex_);
      cv_.wait(lock, [this] { return stop_ || !tasks_.empty(); });
      if (stop_ && tasks_.empty()) {
        return;
      }
      task = std::move(tasks_.front());
      tasks_.pop();
    }
    task();
  }
}
```

### `src/storage/kv_engine.h`

```cpp
#pragma once

#include <memory>
#include <mutex>
#include <string>
#include <vector>

#include "storage/memtable.h"

// Composes Memtable + a list of immutable SSTables + Compactor into one
// coherent, crash-recoverable key-value engine. See sections 1-3 of the
// accompanying doc for the read-order and recovery invariants this class
// is responsible for maintaining.
class KVEngine {
 public:
  // `data_dir`: where WAL and SSTable files live. Created if it doesn't
  // exist; scanned for pre-existing files (crash recovery) if it does.
  explicit KVEngine(const std::string& data_dir,
                     size_t memtable_threshold_bytes = 4 * 1024 * 1024,
                     size_t compaction_threshold_files = 4);

  KVEngine(const KVEngine&) = delete;
  KVEngine& operator=(const KVEngine&) = delete;

  // Returns false only on an underlying WAL write failure — caller must
  // not acknowledge the client in that case.
  bool Put(const std::string& key, const std::string& value);
  bool Delete(const std::string& key);

  // Returns true and sets *out_value if found; false if not present or
  // deleted (indistinguishable to the caller — a tombstone means "not
  // found" from here on).
  bool Get(const std::string& key, std::string* out_value);

 private:
  void MaybeFlush();    // caller must hold mutex_
  void MaybeCompact();  // caller must hold mutex_

  std::string data_dir_;
  size_t memtable_threshold_bytes_;
  size_t compaction_threshold_files_;

  std::mutex mutex_;
  std::unique_ptr<Memtable> memtable_;
  std::vector<std::string> sstable_paths_;  // oldest first, newest at back

  int next_sstable_id_;
  int next_wal_id_;
};
```

### `src/storage/kv_engine.cpp`

```cpp
#include "storage/kv_engine.h"

#include <algorithm>
#include <filesystem>

#include "storage/compaction.h"
#include "storage/sstable.h"

namespace fs = std::filesystem;

namespace {

bool HasSuffix(const std::string& s, const std::string& suffix) {
  return s.size() >= suffix.size() &&
         s.compare(s.size() - suffix.size(), suffix.size(), suffix) == 0;
}

// Parses the numeric id out of a filename like "wal_5.log" or "sst_12.sst"
// given its known prefix/suffix. Returns -1 if the filename doesn't
// actually match the expected pattern (defensive against stray files).
int ParseId(const std::string& filename, const std::string& prefix,
            const std::string& suffix) {
  if (filename.size() <= prefix.size() + suffix.size()) return -1;
  if (filename.compare(0, prefix.size(), prefix) != 0) return -1;
  if (!HasSuffix(filename, suffix)) return -1;

  std::string id_str = filename.substr(
      prefix.size(), filename.size() - prefix.size() - suffix.size());
  try {
    return std::stoi(id_str);
  } catch (...) {
    return -1;
  }
}

}  // namespace

KVEngine::KVEngine(const std::string& data_dir,
                    size_t memtable_threshold_bytes,
                    size_t compaction_threshold_files)
    : data_dir_(data_dir),
      memtable_threshold_bytes_(memtable_threshold_bytes),
      compaction_threshold_files_(compaction_threshold_files),
      next_sstable_id_(0),
      next_wal_id_(0) {
  fs::create_directories(data_dir_);

  std::string existing_wal_path;
  int existing_wal_id = -1;
  std::vector<std::pair<int, std::string>> existing_sstables;

  for (const auto& dir_entry : fs::directory_iterator(data_dir_)) {
    std::string name = dir_entry.path().filename().string();

    int wal_id = ParseId(name, "wal_", ".log");
    if (wal_id >= 0) {
      // Defensive: if more than one WAL file is somehow present, prefer
      // the most recent id. Normal operation only ever has at most one.
      if (wal_id > existing_wal_id) {
        existing_wal_id = wal_id;
        existing_wal_path = dir_entry.path().string();
      }
      continue;
    }

    int sst_id = ParseId(name, "sst_", ".sst");
    if (sst_id >= 0) {
      existing_sstables.emplace_back(sst_id, dir_entry.path().string());
    }
  }

  // Sort by id ascending == oldest-to-newest, matching what both the
  // read path (section 2) and Compactor::CompactAll require.
  std::sort(existing_sstables.begin(), existing_sstables.end());
  for (auto& [id, path] : existing_sstables) {
    sstable_paths_.push_back(path);
    next_sstable_id_ = std::max(next_sstable_id_, id + 1);
  }

  if (existing_wal_id >= 0) {
    // Recovery path: rebuild the memtable exactly as it was pre-crash.
    memtable_ = std::make_unique<Memtable>(existing_wal_path,
                                            memtable_threshold_bytes_);
    memtable_->RecoverFromWal();
    next_wal_id_ = existing_wal_id + 1;
  } else {
    std::string wal_path =
        data_dir_ + "/wal_" + std::to_string(next_wal_id_++) + ".log";
    memtable_ = std::make_unique<Memtable>(wal_path, memtable_threshold_bytes_);
  }
}

bool KVEngine::Put(const std::string& key, const std::string& value) {
  std::lock_guard<std::mutex> lock(mutex_);
  if (!memtable_->Put(key, value)) {
    return false;
  }
  MaybeFlush();
  return true;
}

bool KVEngine::Delete(const std::string& key) {
  std::lock_guard<std::mutex> lock(mutex_);
  if (!memtable_->Delete(key)) {
    return false;
  }
  MaybeFlush();
  return true;
}

bool KVEngine::Get(const std::string& key, std::string* out_value) {
  std::lock_guard<std::mutex> lock(mutex_);

  GetResult mt_result = memtable_->Get(key, out_value);
  if (mt_result == GetResult::kFound) return true;
  if (mt_result == GetResult::kDeleted) return false;

  // kNotPresent: fall through to SSTables, newest first (section 2).
  for (auto it = sstable_paths_.rbegin(); it != sstable_paths_.rend(); ++it) {
    SSTable sst(*it);
    SSTableGetResult sst_result = sst.Get(key, out_value);
    if (sst_result == SSTableGetResult::kFound) return true;
    if (sst_result == SSTableGetResult::kDeleted) return false;
    // kNotPresent: keep checking older files.
  }

  return false;
}

void KVEngine::MaybeFlush() {
  if (!memtable_->IsFull()) {
    return;
  }

  std::string sst_path =
      data_dir_ + "/sst_" + std::to_string(next_sstable_id_++) + ".sst";
  std::vector<MemtableEntry> entries = memtable_->AllSorted();

  if (!SSTable::WriteFromEntries(sst_path, entries)) {
    // Flush failed — roll back the id and keep writing to the current
    // memtable/WAL rather than losing data or corrupting the id sequence.
    next_sstable_id_--;
    return;
  }

  // Safe only now: entries are durably in sst_path, so the WAL that
  // produced them is no longer needed for recovery.
  memtable_->GetWal().Remove();
  sstable_paths_.push_back(sst_path);

  std::string wal_path =
      data_dir_ + "/wal_" + std::to_string(next_wal_id_++) + ".log";
  memtable_ = std::make_unique<Memtable>(wal_path, memtable_threshold_bytes_);

  MaybeCompact();
}

void KVEngine::MaybeCompact() {
  if (!Compactor::ShouldCompact(sstable_paths_.size(),
                                 compaction_threshold_files_)) {
    return;
  }

  std::string compacted_path =
      data_dir_ + "/sst_" + std::to_string(next_sstable_id_++) + ".sst";

  if (Compactor::CompactAll(sstable_paths_, compacted_path,
                             /*delete_inputs_on_success=*/true)) {
    sstable_paths_.clear();
    sstable_paths_.push_back(compacted_path);
  } else {
    next_sstable_id_--;
  }
}
```

### `src/net/tcp_server.h`

```cpp
#pragma once

#include <atomic>
#include <string>
#include <thread>

#include "net/thread_pool.h"
#include "storage/kv_engine.h"

// Thread-pool TCP server exposing KVEngine over a simple line-based text
// protocol. See section 6 of the accompanying doc for the protocol and
// its deliberate limitations.
class TCPServer {
 public:
  TCPServer(int port, KVEngine* engine, size_t thread_pool_size = 4);
  ~TCPServer();

  TCPServer(const TCPServer&) = delete;
  TCPServer& operator=(const TCPServer&) = delete;

  // Binds, listens, and starts accepting connections on a background
  // thread. Returns false on any socket setup failure.
  bool Start();

  // Stops accepting new connections and joins the accept thread. Safe to
  // call multiple times; safe to skip if Start() was never called or
  // failed. The destructor calls this too.
  void Stop();

 private:
  void AcceptLoop();
  void HandleClient(int client_fd);
  std::string HandleCommand(const std::string& line);

  int port_;
  KVEngine* engine_;
  ThreadPool pool_;
  int listen_fd_;
  std::atomic<bool> running_;
  std::thread accept_thread_;
};
```

### `src/net/tcp_server.cpp`

```cpp
#include "net/tcp_server.h"

#include <arpa/inet.h>
#include <netinet/in.h>
#include <sys/socket.h>
#include <unistd.h>

#include <cctype>
#include <cstring>
#include <sstream>

TCPServer::TCPServer(int port, KVEngine* engine, size_t thread_pool_size)
    : port_(port),
      engine_(engine),
      pool_(thread_pool_size),
      listen_fd_(-1),
      running_(false) {}

TCPServer::~TCPServer() { Stop(); }

bool TCPServer::Start() {
  listen_fd_ = socket(AF_INET, SOCK_STREAM, 0);
  if (listen_fd_ < 0) {
    return false;
  }

  int opt = 1;
  setsockopt(listen_fd_, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt));

  sockaddr_in addr{};
  addr.sin_family = AF_INET;
  addr.sin_addr.s_addr = INADDR_ANY;
  addr.sin_port = htons(static_cast<uint16_t>(port_));

  if (bind(listen_fd_, reinterpret_cast<sockaddr*>(&addr), sizeof(addr)) < 0) {
    close(listen_fd_);
    return false;
  }
  if (listen(listen_fd_, /*backlog=*/16) < 0) {
    close(listen_fd_);
    return false;
  }

  running_ = true;
  accept_thread_ = std::thread(&TCPServer::AcceptLoop, this);
  return true;
}

void TCPServer::AcceptLoop() {
  while (running_) {
    sockaddr_in client_addr{};
    socklen_t client_len = sizeof(client_addr);
    int client_fd = accept(listen_fd_, reinterpret_cast<sockaddr*>(&client_addr),
                            &client_len);
    if (client_fd < 0) {
      // Stop() closes listen_fd_, which unblocks accept() with an error —
      // that's the expected shutdown path, not a real failure.
      if (!running_) break;
      continue;
    }
    pool_.Enqueue([this, client_fd] { HandleClient(client_fd); });
  }
}

void TCPServer::HandleClient(int client_fd) {
  std::string buffer;
  char chunk[4096];

  while (running_) {
    ssize_t n = recv(client_fd, chunk, sizeof(chunk), 0);
    if (n <= 0) break;  // 0 = client closed; <0 = socket error

    buffer.append(chunk, static_cast<size_t>(n));

    size_t newline_pos;
    while ((newline_pos = buffer.find('\n')) != std::string::npos) {
      std::string line = buffer.substr(0, newline_pos);
      buffer.erase(0, newline_pos + 1);
      if (!line.empty() && line.back() == '\r') {
        line.pop_back();  // tolerate CRLF from tools like telnet
      }

      std::string response = HandleCommand(line);
      send(client_fd, response.data(), response.size(), 0);
    }
  }

  close(client_fd);
}

std::string TCPServer::HandleCommand(const std::string& line) {
  std::istringstream iss(line);
  std::string cmd;
  iss >> cmd;
  for (auto& c : cmd) c = static_cast<char>(std::toupper(c));

  if (cmd == "PUT") {
    std::string key, value;
    iss >> key >> value;
    if (key.empty() || value.empty()) return "ERROR bad_request\n";
    return engine_->Put(key, value) ? "OK\n" : "ERROR write_failed\n";
  }

  if (cmd == "GET") {
    std::string key;
    iss >> key;
    if (key.empty()) return "ERROR bad_request\n";
    std::string value;
    if (engine_->Get(key, &value)) return "VALUE " + value + "\n";
    return "NOT_FOUND\n";
  }

  if (cmd == "DELETE") {
    std::string key;
    iss >> key;
    if (key.empty()) return "ERROR bad_request\n";
    return engine_->Delete(key) ? "OK\n" : "ERROR write_failed\n";
  }

  return "ERROR unknown_command\n";
}

void TCPServer::Stop() {
  if (!running_) return;
  running_ = false;
  shutdown(listen_fd_, SHUT_RDWR);
  close(listen_fd_);
  if (accept_thread_.joinable()) {
    accept_thread_.join();
  }
}
```

### `src/server_main.cpp`

```cpp
#include <csignal>
#include <cstdlib>
#include <chrono>
#include <iostream>
#include <thread>

#include "net/tcp_server.h"
#include "storage/kv_engine.h"

namespace {
TCPServer* g_server = nullptr;

void HandleSigint(int) {
  if (g_server != nullptr) {
    g_server->Stop();
  }
  std::exit(0);
}
}  // namespace

int main(int argc, char** argv) {
  int port = 9090;
  std::string data_dir = "kv_data";
  if (argc > 1) port = std::atoi(argv[1]);
  if (argc > 2) data_dir = argv[2];

  KVEngine engine(data_dir);
  TCPServer server(port, &engine);
  g_server = &server;
  std::signal(SIGINT, HandleSigint);

  if (!server.Start()) {
    std::cerr << "Failed to start server on port " << port << "\n";
    return 1;
  }

  std::cout << "KV store listening on port " << port
            << " (data dir: " << data_dir << ")\n";
  std::cout << "Press Ctrl+C to stop.\n";

  while (true) {
    std::this_thread::sleep_for(std::chrono::seconds(3600));
  }
  return 0;
}
```

### `tools/cli.cpp`

```cpp
#include <arpa/inet.h>
#include <netinet/in.h>
#include <sys/socket.h>
#include <unistd.h>

#include <cstdlib>
#include <iostream>
#include <sstream>
#include <string>

int main(int argc, char** argv) {
  if (argc < 4) {
    std::cerr << "Usage: " << argv[0]
              << " <host> <port> <PUT key value | GET key | DELETE key>\n";
    return 1;
  }

  std::string host = argv[1];
  int port = std::atoi(argv[2]);

  std::ostringstream command;
  for (int i = 3; i < argc; i++) {
    command << argv[i];
    if (i + 1 < argc) command << " ";
  }
  command << "\n";

  int sock = socket(AF_INET, SOCK_STREAM, 0);
  sockaddr_in addr{};
  addr.sin_family = AF_INET;
  addr.sin_port = htons(static_cast<uint16_t>(port));
  inet_pton(AF_INET, host.c_str(), &addr.sin_addr);

  if (connect(sock, reinterpret_cast<sockaddr*>(&addr), sizeof(addr)) < 0) {
    std::cerr << "Could not connect to " << host << ":" << port << "\n";
    return 1;
  }

  std::string cmd_str = command.str();
  send(sock, cmd_str.data(), cmd_str.size(), 0);

  char buf[4096];
  ssize_t n = recv(sock, buf, sizeof(buf), 0);
  if (n > 0) {
    std::cout.write(buf, n);
  }

  close(sock);
  return 0;
}
```

### `test/integration/tcp_server_test.cpp`

```cpp
#include <arpa/inet.h>
#include <netinet/in.h>
#include <sys/socket.h>
#include <unistd.h>

#include <cassert>
#include <chrono>
#include <filesystem>
#include <iostream>
#include <string>
#include <thread>
#include <vector>

#include "net/tcp_server.h"
#include "storage/kv_engine.h"

namespace {

std::string SendCommand(int port, const std::string& command) {
  int sock = socket(AF_INET, SOCK_STREAM, 0);
  sockaddr_in addr{};
  addr.sin_family = AF_INET;
  addr.sin_port = htons(static_cast<uint16_t>(port));
  inet_pton(AF_INET, "127.0.0.1", &addr.sin_addr);

  if (connect(sock, reinterpret_cast<sockaddr*>(&addr), sizeof(addr)) < 0) {
    return "CONNECT_FAILED";
  }

  std::string full_command = command + "\n";
  send(sock, full_command.data(), full_command.size(), 0);

  char buf[4096];
  ssize_t n = recv(sock, buf, sizeof(buf), 0);
  close(sock);
  return (n > 0) ? std::string(buf, static_cast<size_t>(n)) : "";
}

}  // namespace

int main() {
  const std::string kDataDir = "test_tcp_server_data";
  std::filesystem::remove_all(kDataDir);

  const int kPort = 9191;
  KVEngine engine(kDataDir);
  TCPServer server(kPort, &engine, /*thread_pool_size=*/4);
  assert(server.Start());

  // Give the accept loop a moment to actually be listening before the
  // first client connects.
  std::this_thread::sleep_for(std::chrono::milliseconds(100));

  // --- Basic protocol correctness ---
  assert(SendCommand(kPort, "PUT apple red") == "OK\n");
  assert(SendCommand(kPort, "GET apple") == "VALUE red\n");
  assert(SendCommand(kPort, "GET banana") == "NOT_FOUND\n");
  assert(SendCommand(kPort, "DELETE apple") == "OK\n");
  assert(SendCommand(kPort, "GET apple") == "NOT_FOUND\n");
  assert(SendCommand(kPort, "BOGUS foo") == "ERROR unknown_command\n");
  assert(SendCommand(kPort, "PUT keyonly") == "ERROR bad_request\n");

  // --- Concurrency smoke test: several clients through the pool at once ---
  std::vector<std::thread> clients;
  for (int i = 0; i < 10; i++) {
    clients.emplace_back([kPort, i] {
      std::string key = "concurrent" + std::to_string(i);
      std::string expected_value = "value" + std::to_string(i);
      assert(SendCommand(kPort, "PUT " + key + " " + expected_value) ==
             "OK\n");
      assert(SendCommand(kPort, "GET " + key) ==
             "VALUE " + expected_value + "\n");
    });
  }
  for (auto& t : clients) t.join();

  server.Stop();
  std::filesystem::remove_all(kDataDir);

  std::cout << "All TCP server tests passed.\n";
  return 0;
}
```

---

## 9. Things Worth Understanding, Not Just Running

- **Why does `MaybeFlush` create the new `Memtable` only *after*
  `WriteFromEntries` succeeds and the old WAL is removed?** Same
  crash-safety ordering principle as every prior step: if the process
  died in between, you want the state on disk to always be
  recoverable into *something* correct — either "old WAL still there,
  new SSTable doesn't exist yet" (recovery replays the old WAL, fine)
  or "old WAL gone, new SSTable exists" (recovery finds the SSTable,
  fine). There's no ordering here that leaves you with data existing
  nowhere.

- **Why does `Get` construct a fresh `SSTable` object for every file,
  on every call, instead of keeping them open/cached?** Simplicity,
  deliberately, for this phase — each `SSTable` construction re-reads
  its sparse index and bloom filter off disk, which is wasted repeated
  work under real load. This is a genuine, nameable optimization
  opportunity (keep a `vector<unique_ptr<SSTable>>` alongside the path
  list, reused across calls) that's fair to defer, since correctness
  here doesn't depend on it — a good "if I had more time" line for an
  interview, not a bug.

- **Why does the accept loop's error handling check `!running_`
  specifically, instead of treating every `accept()` failure the
  same way?** Because `Stop()`'s shutdown mechanism *works* by closing
  `listen_fd_` out from under a blocked `accept()` call — that's the
  clean way to unblock a thread stuck in a blocking syscall. Without
  distinguishing "accept failed because we're shutting down" from
  "accept failed for some other real reason," you'd either loop
  forever retrying after shutdown, or treat normal shutdown as an
  error worth logging/alerting on.

- **What would break under real concurrent load that this test
  doesn't catch?** The concurrency smoke test here uses distinct keys
  per thread — it proves the mutex prevents corruption, but doesn't
  stress contention on the *same* key from multiple clients
  simultaneously, or interleave flush/compaction with concurrent
  reads under real timing pressure. That's precisely what Phase 4's
  chaos testing exists to probe properly later — worth remembering
  this test is a correctness smoke test, not a substitute for that.

---

## 10. Try This Yourself (Phase 1's Actual Milestone)

Once `test_tcp_server` passes, do the real thing the design doc's
milestone describes, by hand:

```bash
./build/kv_server 9090 kv_data &
./build/cli 127.0.0.1 9090 PUT apple red
./build/cli 127.0.0.1 9090 PUT banana yellow
./build/cli 127.0.0.1 9090 GET apple      # should print: VALUE red

kill -9 %1                                 # simulate a hard crash

./build/kv_server 9090 kv_data &
./build/cli 127.0.0.1 9090 GET apple      # should STILL print: VALUE red
./build/cli 127.0.0.1 9090 GET banana     # should STILL print: VALUE yellow
```

If both values survive the `kill -9`, you've just demonstrated —
manually, with your own hands, not just via a passing test — the exact
milestone your project plan describes for Phase 1: *"A single-node KV
store that survives a `kill -9` mid-write without losing acknowledged
data."* Worth recording this (a terminal screenshot, or the exact
transcript) for your eventual README.

---

## 11. Phase 1 Is Complete — What's Next

Every step in design doc §1.6 is done. You have a real, working,
crash-safe, single-node LSM-tree key-value store with a network
interface. That's genuinely a complete, demo-able system on its own —
worth pausing to actually feel that, not just rushing ahead.

**Next: Phase 2 — Replication via Raft** (design doc §2, build order
§2.8). This is, per your own project plan, *the* hard part — budgeted
2-4x longer than expected for most people, and the part with the
highest interview signal value. It starts with the RPC framework
(`RequestVote` and `AppendEntries` as serializable structs) and a
state machine skeleton tested with fake, in-process network calls
before any real sockets are involved — deliberately mirroring the same
"prove each layer correct in isolation first" discipline you've been
using through all of Phase 1.
