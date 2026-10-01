# Memtable — Concepts & Full Implementation

**Phase 1, Step 3** of the build order. This is where the skip list and
WAL you already built stop being standalone pieces and become the real
write path from the design doc's §1.3. `SSTable` doesn't exist yet, so
this step stops at "the memtable knows it's full and freezes itself" —
actually flushing to disk is the next step.

Same habit as before: trace one `Put` and one `Delete` through this
class by hand — which calls hit the WAL, which hit the skip list, in
what order — before you just build and run it.

---

## 1. What a Memtable Actually Is

A memtable is not a new data structure — it's the skip list from step 1,
wrapped with three things the skip list alone doesn't know about:

1. **Write-ahead logging** — every write goes to the WAL before the
   skip list, so a crash can be replayed back into an equivalent state.
2. **Tombstones for deletes** — a `Delete` doesn't remove anything;
   it inserts a special marker meaning "this key is deleted as of this
   point in time."
3. **A size trigger** — once the memtable holds roughly `threshold`
   bytes, it should stop accepting writes and get flushed to an
   immutable SSTable (next step), making room for a fresh memtable.

This matches the write path in the design doc exactly:

```
1. Append record to WAL, fsync.
2. Insert into memtable (skip list).
3. Acknowledge client.
4. (Async, background) When memtable exceeds threshold: freeze it,
   start a new memtable, flush the frozen one to an SSTable, delete
   the WAL segment once the flush is durable.
```

Steps 1–3 are what you're building now. Step 4's *flush* is next step;
today's `Memtable` only needs to correctly report "I'm full" so the
caller can freeze it.

---

## 2. Tombstones: Why Delete Doesn't Delete

Say `key="apple"` was written a while ago, and the memtable holding
that write has already been flushed to SSTable #3 on disk. Now you
call `Delete("apple")` on the *current* memtable. If `Delete` simply
removed `"apple"` from the current (empty, since it was never in this
one) skip list, nothing would actually record that the key was
deleted — a `Get("apple")` would check the current memtable (not
found), then fall through to SSTable #3, find the old value, and
incorrectly return it as if the delete never happened.

The fix: `Delete` inserts a **tombstone** — a record marked
`OpType::kDelete` with no meaningful value — at the current position
in the ordering. Now `Get("apple")` checks the current memtable first,
finds the tombstone, and correctly reports "not found" *without ever
falling through to the stale SSTable value*. The tombstone itself gets
permanently removed later, during compaction, once you're certain no
older SSTable still holds that key.

This is why the memtable's `Get` needs to distinguish three outcomes,
not two: **found with a value**, **found as a tombstone** (definitely
deleted, stop searching), and **not present in this memtable at all**
(caller should keep checking older memtables/SSTables).

---

## 3. Approximate Size Tracking

Computing the memtable's exact byte size by walking every node in the
skip list on every single write would be O(n) per write — directly
defeating the O(log n) insert you built the skip list for in the first
place.

Instead: keep a running counter (`approximate_size_`), and on every
`Put`/`Delete`, add a rough estimate of that record's footprint (key
bytes + value bytes + some fixed overhead for the skip list node
itself) to the counter. This is *approximate* on purpose — real
systems (RocksDB included) do the same thing, because the flush
threshold doesn't need to be exact to byte, it just needs to trigger
"roughly around here" reliably. Being off by a small constant overhead
per node doesn't change the amortized behavior.

---

## 4. Freezing Instead of Flushing Inline

Per §1.3, flushing to disk is meant to happen **asynchronously**, in
the background — a client whose write happened to be the one that
crossed the threshold shouldn't have to wait for a multi-MB disk flush
before getting acknowledged.

There's no background thread yet (that's implicit infrastructure you
don't have until later), so for now `Memtable` supports the *shape* of
that design without the async part:

- `IsFull()` tells the caller "this instance has crossed the
  threshold."
- The caller (later: your TCP server's write handler, eventually a
  background flush thread) is responsible for swapping in a fresh
  `Memtable` and keeping the frozen one around until something flushes
  it to an SSTable.
- `Memtable` itself has no idea what happens to it after it's frozen —
  that separation is deliberate, so this class stays testable in
  isolation, same as the skip list and WAL before it.

---

## 5. Build Setup

Full `CMakeLists.txt` — replace your existing file with this (adds
`memtable.cpp` and `test_memtable` alongside what you already have):

```cmake
cmake_minimum_required(VERSION 3.16)
project(distributed_kv_store CXX)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -Wall -Wextra -g")

enable_testing()

add_library(storage
    src/storage/skiplist.cpp
    src/storage/wal.cpp
    src/storage/memtable.cpp
)
target_include_directories(storage PUBLIC src)

add_executable(test_skiplist test/unit/storage/test_skiplist.cpp)
target_link_libraries(test_skiplist PRIVATE storage)
add_test(NAME test_skiplist COMMAND test_skiplist)

add_executable(test_wal test/unit/storage/test_wal.cpp)
target_link_libraries(test_wal PRIVATE storage)
add_test(NAME test_wal COMMAND test_wal)

add_executable(test_memtable test/unit/storage/test_memtable.cpp)
target_link_libraries(test_memtable PRIVATE storage)
add_test(NAME test_memtable COMMAND test_memtable)
```

Build and run:

```bash
cmake -B build
cmake --build build
./build/test_memtable
```

---

## 6. Full Code

### `src/storage/memtable.h`

```cpp
#pragma once

#include <cstddef>
#include <string>

#include "storage/skiplist.h"
#include "storage/wal.h"

enum class GetResult {
  kFound,       // key present with a real value
  kDeleted,     // key present as a tombstone — definitely deleted
  kNotPresent,  // key not in this memtable at all; caller should keep
                // checking older memtables/SSTables
};

// Wraps a skip list with write-ahead logging and a size-threshold flush
// trigger. This is the in-memory half of the LSM-tree write path from
// design doc §1.3, steps 1-3. Step 4 (actual flush to SSTable) happens
// one level up, once SSTable exists.
class Memtable {
 public:
  // `wal_path`: where this memtable's WAL segment lives.
  // `size_threshold_bytes`: approximate size at which IsFull() starts
  // returning true (design doc suggests 4-64MB in production; use a
  // small value like 1024 in tests so you don't need to write megabytes
  // of data to exercise the flush-trigger path).
  Memtable(const std::string& wal_path, size_t size_threshold_bytes);

  Memtable(const Memtable&) = delete;
  Memtable& operator=(const Memtable&) = delete;

  // Writes to WAL first (fsync'd), then inserts into the skip list.
  // Returns false if the WAL write failed — caller must NOT acknowledge
  // the client in that case.
  bool Put(const std::string& key, const std::string& value);

  // Writes a tombstone to WAL first, then inserts the tombstone marker
  // into the skip list. See section 2 for why this doesn't remove
  // anything.
  bool Delete(const std::string& key);

  // Looks up `key`. See GetResult for the three possible outcomes.
  GetResult Get(const std::string& key, std::string* out_value) const;

  // True once approximate_size_ has crossed the configured threshold.
  // Caller should freeze this instance (stop writing to it) and swap in
  // a fresh Memtable once this returns true.
  bool IsFull() const;

  size_t ApproximateSize() const { return approximate_size_; }

  // Rebuilds this memtable's state by replaying its WAL — used on
  // startup to recover whatever was in memory when the process last
  // exited/crashed. Returns the number of records replayed.
  size_t RecoverFromWal();

  // Exposes the underlying WAL so the caller can Remove() it once this
  // memtable has been durably flushed to an SSTable (not yet wired up
  // until SSTable exists).
  Wal& GetWal() { return wal_; }

 private:
  // Fixed per-record overhead estimate for the skip list node itself
  // (pointers array, struct overhead) on top of key+value bytes. Doesn't
  // need to be exact — see section 3.
  static constexpr size_t kPerRecordOverhead = 32;

  void ApplyToSkipList(const std::string& key, const std::string& value,
                        OpType op_type);

  SkipList skiplist_;
  Wal wal_;
  size_t size_threshold_bytes_;
  size_t approximate_size_;

  // Sentinel value stored for tombstones, distinguishable from any real
  // value by checking the accompanying op_type, not by content.
  static const std::string kTombstoneMarker;
};
```

### `src/storage/memtable.cpp`

```cpp
#include "storage/memtable.h"

const std::string Memtable::kTombstoneMarker = "";

Memtable::Memtable(const std::string& wal_path, size_t size_threshold_bytes)
    : wal_(wal_path),
      size_threshold_bytes_(size_threshold_bytes),
      approximate_size_(0) {}

void Memtable::ApplyToSkipList(const std::string& key,
                                const std::string& value, OpType op_type) {
  // The skip list itself doesn't know about tombstones — it just stores
  // key -> value. We encode "this is a tombstone" by storing a tiny
  // sentinel and relying on Get() below to never trust the skip list's
  // value directly without knowing which op_type produced it. Since the
  // skip list has no op_type field, we prefix the stored value with a
  // 1-byte tag instead, so Get() can tell them apart after the fact.
  std::string tagged_value;
  tagged_value.reserve(value.size() + 1);
  tagged_value.push_back(static_cast<char>(op_type));
  tagged_value += value;

  skiplist_.Insert(key, tagged_value);
  approximate_size_ += key.size() + value.size() + kPerRecordOverhead;
}

bool Memtable::Put(const std::string& key, const std::string& value) {
  if (!wal_.Append(key, value, OpType::kPut)) {
    return false;
  }
  ApplyToSkipList(key, value, OpType::kPut);
  return true;
}

bool Memtable::Delete(const std::string& key) {
  if (!wal_.Append(key, kTombstoneMarker, OpType::kDelete)) {
    return false;
  }
  ApplyToSkipList(key, kTombstoneMarker, OpType::kDelete);
  return true;
}

GetResult Memtable::Get(const std::string& key, std::string* out_value) const {
  std::string tagged_value;
  if (!skiplist_.Search(key, &tagged_value)) {
    return GetResult::kNotPresent;
  }

  // First byte is the op_type tag we prefixed in ApplyToSkipList.
  OpType op_type = static_cast<OpType>(tagged_value[0]);
  if (op_type == OpType::kDelete) {
    return GetResult::kDeleted;
  }

  if (out_value != nullptr) {
    *out_value = tagged_value.substr(1);
  }
  return GetResult::kFound;
}

bool Memtable::IsFull() const {
  return approximate_size_ >= size_threshold_bytes_;
}

size_t Memtable::RecoverFromWal() {
  return wal_.Replay([this](const WalRecord& record) {
    ApplyToSkipList(record.key, record.value, record.op_type);
  });
}
```

### `test/unit/storage/test_memtable.cpp`

```cpp
#include <cassert>
#include <cstdio>
#include <iostream>
#include "storage/memtable.h"

namespace {
const char* kTestPath = "test_memtable_scratch.log";
}

int main() {
  std::remove(kTestPath);

  // --- Put + Get ---
  {
    Memtable mt(kTestPath, /*size_threshold_bytes=*/1024);

    assert(mt.Put("apple", "red"));
    assert(mt.Put("banana", "yellow"));

    std::string v;
    assert(mt.Get("apple", &v) == GetResult::kFound && v == "red");
    assert(mt.Get("banana", &v) == GetResult::kFound && v == "yellow");
    assert(mt.Get("durian", &v) == GetResult::kNotPresent);
  }

  // --- Overwrite ---
  std::remove(kTestPath);
  {
    Memtable mt(kTestPath, 1024);
    assert(mt.Put("apple", "red"));
    assert(mt.Put("apple", "green"));

    std::string v;
    assert(mt.Get("apple", &v) == GetResult::kFound && v == "green");
  }

  // --- Tombstones: Delete must NOT make the key look kNotPresent ---
  std::remove(kTestPath);
  {
    Memtable mt(kTestPath, 1024);
    assert(mt.Put("apple", "red"));
    assert(mt.Delete("apple"));

    std::string v;
    // Crucial distinction: kDeleted, not kNotPresent. A caller checking
    // older SSTables needs to know "stop, this is deleted" versus
    // "keep looking, this memtable just doesn't have it."
    assert(mt.Get("apple", &v) == GetResult::kDeleted);

    // A key that was truly never written is still kNotPresent.
    assert(mt.Get("never_written", &v) == GetResult::kNotPresent);
  }

  // --- Delete-then-Put resurrects the key ---
  std::remove(kTestPath);
  {
    Memtable mt(kTestPath, 1024);
    assert(mt.Put("apple", "red"));
    assert(mt.Delete("apple"));
    assert(mt.Put("apple", "blue"));

    std::string v;
    assert(mt.Get("apple", &v) == GetResult::kFound && v == "blue");
  }

  // --- IsFull() triggers once threshold is crossed ---
  std::remove(kTestPath);
  {
    Memtable mt(kTestPath, /*size_threshold_bytes=*/100);
    assert(!mt.IsFull());

    int i = 0;
    while (!mt.IsFull()) {
      mt.Put("key" + std::to_string(i), "some_value_here");
      i++;
      assert(i < 1000);  // safety net against an infinite loop if broken
    }
    assert(mt.IsFull());
    assert(mt.ApproximateSize() >= 100);
  }

  // --- RecoverFromWal reconstructs state after a simulated crash ---
  std::remove(kTestPath);
  {
    Memtable mt(kTestPath, 1024);
    assert(mt.Put("apple", "red"));
    assert(mt.Put("banana", "yellow"));
    assert(mt.Delete("apple"));
    // mt goes out of scope here — simulates process exit/crash. The
    // skip list's in-memory state is gone; only the WAL on disk remains.
  }
  {
    Memtable recovered(kTestPath, 1024);
    size_t n = recovered.RecoverFromWal();
    assert(n == 3);

    std::string v;
    assert(recovered.Get("apple", &v) == GetResult::kDeleted);
    assert(recovered.Get("banana", &v) == GetResult::kFound && v == "yellow");
  }

  std::cout << "All memtable tests passed.\n";
  return 0;
}
```

---

## 7. Things Worth Understanding, Not Just Running

- **Why tag the value with `op_type` instead of giving the skip list
  its own concept of tombstones?** The skip list is a generic,
  reusable key→string structure — it shouldn't need to know anything
  about WAL semantics or LSM-tree concepts. Keeping that separation
  means the skip list stays simple and independently testable (which
  it already is, from step 1), and all the "what does this value
  *mean*" logic lives in `Memtable`, where it belongs.

- **Why does `RecoverFromWal` call the same `ApplyToSkipList` that
  `Put`/`Delete` use, instead of writing separate recovery logic?**
  Because recovery should reconstruct *exactly* the same in-memory
  state that existed pre-crash — reusing the same code path that
  built that state the first time is what guarantees that, rather
  than trusting two separate implementations to stay in sync.

- **Why doesn't `RecoverFromWal` call `Wal::Append` again for each
  record?** The records are already durably in the WAL file — they're
  being replayed *from* it. Re-appending them would duplicate every
  record in the log on every restart, growing the WAL unboundedly for
  no reason.

- **What's still missing before this can flush to disk?** `SSTable`
  itself. Once it exists, the real Phase 1 step 4 wiring becomes:
  caller checks `IsFull()`, freezes the current `Memtable`, constructs
  a new one for future writes, and asynchronously calls
  `SortedKeys()`-style iteration over the frozen one to write an
  SSTable file — then, only once that flush is confirmed durable,
  calls `GetWal().Remove()` on the frozen memtable's WAL.

---

## 8. Next Step (Phase 1, Step 4)

Per the build order: **SSTable writer** (from a sorted memtable) and
**reader** (binary search into the sparse index + block scan), per
design doc §1.2's SSTable structure — data blocks, sparse index, bloom
filter, footer, in that file layout. This is the piece that finally
lets a frozen `Memtable` actually leave memory and become durable.
