# Compaction — Concepts & Full Implementation

**Phase 1, Step 6** of the build order (design doc §1.6): *"Compaction
(start with simplest possible: merge-all when L0 file count exceeds N;
add proper leveled logic once basic correctness is proven)."* This is
the last Phase 1 storage piece — once this passes, you're at step 7:
wiring the whole engine behind a TCP server, and Phase 1 is complete.

This one has a real correctness subtlety at its core (resolving
multiple versions of the same key, and knowing when a tombstone is
safe to discard), so trace through the worked example in section 3 by
hand before running anything — it's short, and it's the part that
actually matters here.

---

## 1. The Problem This Solves

Every memtable flush creates a brand-new, immutable SSTable file
(§1.3). Over time, without compaction, a single `Get` would need to
check an ever-growing number of files — no bound at all. Worse: the
**same key** can legitimately appear in multiple SSTables, each
representing what was true at a different point in time (an update, a
delete, another update...). Right now, your SSTable's `Get` only knows
how to look inside *one* file — nothing yet resolves "which of these
five files has the truth for this key" across many files.

Compaction is what merges old SSTables into fewer, larger ones,
**bounding the number of files a read has to check**, while correctly
collapsing multiple historical versions of the same key down to just
the one that still matters.

## 2. Why "Merge-All" First, Not Real Leveled Compaction

The design doc is explicit that leveled compaction (L0, L1, L2...,
each ~10x larger, non-overlapping within a level) is the eventual
target, but tells you to build the simplest version first: whenever
the number of L0 files crosses some threshold `N`, merge **all** of
them into a single new file. This matters for a concrete reason
beyond "simpler is easier to build": **merging *all* known SSTables at
once is the one case where a compactor is safely allowed to drop
tombstones outright** — see section 4. Leveled compaction (later) only
merges *some* files into the next level and has to keep tombstones
around for longer, which is a meaningfully harder problem. Building
merge-all first isn't just a smaller version of the real thing — it's
the version where the trickiest part of the problem doesn't arise yet.

---

## 3. Resolving Multiple Versions of a Key — Worked Example

Say you have three SSTables, oldest to newest:

```
sstable_1 (oldest):  apple -> "red"       (Put)
sstable_2 (middle):  apple -> "green"     (Put)
                      banana -> "yellow"   (Put)
sstable_3 (newest):  banana -> <tombstone> (Delete)
                      cherry -> "dark red" (Put)
```

Merging all three, the correct output is:

```
apple  -> "green"     (sstable_2's Put wins — it's newer than sstable_1's)
banana -> (dropped entirely — sstable_3's Delete is newest, and since
           this merge includes every SSTable that exists, there is no
           older file left anywhere that could still incorrectly
           surface "yellow")
cherry -> "dark red"
```

**The rule**: for each key, take the version from the **newest** file
that contains it. If that newest version is a tombstone, and this
compaction is merging every SSTable that currently exists (the
merge-all case), drop the key from the output entirely — don't even
write the tombstone forward. There's nothing left "underneath" this
merged file that a future `Get` could wrongly fall through to, so the
tombstone has done its job and can be discarded for good.

---

## 4. Why Dropping Tombstones Here Is Safe (and Not Always)

From §1.3: *"Tombstones themselves get removed during compaction once
you're sure no older SSTable has that key."* The keyword is **sure**.

In this merge-all scheme, when compaction runs, it consumes **every
SSTable file that currently exists** and produces **one** new file
replacing all of them. After it finishes, there is no file left in the
system older than the merge — so "no older SSTable has that key" is
trivially guaranteed by construction, for every key, every time. That's
what makes it safe to drop tombstones unconditionally here.

This stops being true the moment you build real leveled compaction:
merging L0 into L1 might leave L2 and L3 still holding an old value for
a key a tombstone in L0 was suppressing. A tombstone can only be
dropped once you know it's been merged all the way down past every
level that could still hold the stale value — a genuinely harder
bookkeeping problem, which is exactly why the design doc defers it.
Worth being able to explain this distinction if asked in an interview;
it's the actual interesting part of compaction correctness.

---

## 5. Adding `SSTable::AllEntries()`

Compaction needs to read *every* record out of an SSTable, not look up
one key — something `SSTable` doesn't support yet (`Get` only). This
adds one read-only method that walks the data section start to finish.

**This replaces your existing `sstable.h` and `sstable.cpp` wholesale**
— the only change is the new `AllEntries()` method; everything else
(`WriteFromEntries`, the constructor, `Get`) is identical to what you
already have and already tested.

### `src/storage/sstable.h`

```cpp
#pragma once

#include <cstdint>
#include <string>
#include <vector>

#include "storage/bloom_filter.h"
#include "storage/memtable.h"

enum class SSTableGetResult {
  kFound,
  kDeleted,
  kNotPresent,
};

class SSTable {
 public:
  static bool WriteFromEntries(const std::string& path,
                                const std::vector<MemtableEntry>& entries);

  explicit SSTable(const std::string& path);

  SSTableGetResult Get(const std::string& key, std::string* out_value) const;

  // Reads every record in this file, in sorted order, decoded back into
  // MemtableEntry form (key, value, op_type — including tombstones, which
  // are returned as-is, not filtered out here). Used by compaction to
  // merge multiple SSTables together.
  std::vector<MemtableEntry> AllEntries() const;

  const std::string& Path() const { return path_; }

 private:
  struct IndexEntry {
    std::string key;
    uint64_t offset;
  };

  static constexpr int kIndexInterval = 16;
  static constexpr uint32_t kMagicNumber = 0x53535442;  // ASCII "SSTB"

  std::string path_;
  std::vector<IndexEntry> sparse_index_;
  BloomFilter bloom_filter_;
  uint64_t data_section_end_;
};
```

### `src/storage/sstable.cpp`

```cpp
#include "storage/sstable.h"

#include <algorithm>
#include <fstream>

namespace {

template <typename T>
void WriteRaw(std::ofstream& out, const T& value) {
  out.write(reinterpret_cast<const char*>(&value), sizeof(T));
}

template <typename T>
bool ReadRaw(std::ifstream& in, T* value) {
  in.read(reinterpret_cast<char*>(value), sizeof(T));
  return static_cast<bool>(in);
}

void WriteLengthPrefixedString(std::ofstream& out, const std::string& s) {
  uint32_t len = static_cast<uint32_t>(s.size());
  WriteRaw(out, len);
  out.write(s.data(), static_cast<std::streamsize>(s.size()));
}

bool ReadLengthPrefixedString(std::ifstream& in, std::string* s) {
  uint32_t len = 0;
  if (!ReadRaw(in, &len)) return false;
  s->assign(len, '\0');
  if (len > 0) {
    in.read(&(*s)[0], len);
    if (!in) return false;
  }
  return true;
}

}  // namespace

bool SSTable::WriteFromEntries(const std::string& path,
                                const std::vector<MemtableEntry>& entries) {
  std::ofstream out(path, std::ios::binary | std::ios::trunc);
  if (!out.is_open()) return false;

  std::vector<IndexEntry> index;
  BloomFilter bloom(entries.empty() ? 1 : entries.size());

  uint64_t offset = 0;
  for (size_t i = 0; i < entries.size(); i++) {
    const MemtableEntry& entry = entries[i];

    if (i % kIndexInterval == 0) {
      index.push_back(IndexEntry{entry.key, offset});
    }
    bloom.Add(entry.key);

    uint32_t key_len = static_cast<uint32_t>(entry.key.size());
    uint32_t val_len = static_cast<uint32_t>(entry.value.size());
    uint8_t op_byte = static_cast<uint8_t>(entry.op_type);

    WriteRaw(out, key_len);
    WriteRaw(out, val_len);
    WriteRaw(out, op_byte);
    out.write(entry.key.data(), static_cast<std::streamsize>(entry.key.size()));
    out.write(entry.value.data(),
              static_cast<std::streamsize>(entry.value.size()));

    offset += sizeof(key_len) + sizeof(val_len) + sizeof(op_byte) +
              entry.key.size() + entry.value.size();
  }

  uint64_t index_offset = offset;

  uint32_t index_count = static_cast<uint32_t>(index.size());
  WriteRaw(out, index_count);
  for (const auto& entry : index) {
    WriteLengthPrefixedString(out, entry.key);
    WriteRaw(out, entry.offset);
  }

  uint64_t bloom_offset = static_cast<uint64_t>(out.tellp());
  uint64_t num_bits = static_cast<uint64_t>(bloom.NumBits());
  int32_t num_hashes = static_cast<int32_t>(bloom.NumHashes());
  const std::vector<uint8_t>& bits = bloom.Bits();
  uint32_t byte_len = static_cast<uint32_t>(bits.size());

  WriteRaw(out, num_bits);
  WriteRaw(out, num_hashes);
  WriteRaw(out, byte_len);
  out.write(reinterpret_cast<const char*>(bits.data()),
            static_cast<std::streamsize>(bits.size()));

  WriteRaw(out, index_offset);
  WriteRaw(out, bloom_offset);
  WriteRaw(out, kMagicNumber);

  out.flush();
  return static_cast<bool>(out);
}

SSTable::SSTable(const std::string& path)
    : path_(path), bloom_filter_(std::vector<uint8_t>{1}, 1) {
  std::ifstream in(path_, std::ios::binary);

  in.seekg(-20, std::ios::end);
  uint64_t index_offset = 0, bloom_offset = 0;
  uint32_t magic = 0;
  ReadRaw(in, &index_offset);
  ReadRaw(in, &bloom_offset);
  ReadRaw(in, &magic);

  data_section_end_ = index_offset;

  in.seekg(static_cast<std::streamoff>(index_offset), std::ios::beg);
  uint32_t index_count = 0;
  ReadRaw(in, &index_count);
  sparse_index_.reserve(index_count);
  for (uint32_t i = 0; i < index_count; i++) {
    IndexEntry entry;
    ReadLengthPrefixedString(in, &entry.key);
    ReadRaw(in, &entry.offset);
    sparse_index_.push_back(std::move(entry));
  }

  in.seekg(static_cast<std::streamoff>(bloom_offset), std::ios::beg);
  uint64_t num_bits = 0;
  int32_t num_hashes = 0;
  uint32_t byte_len = 0;
  ReadRaw(in, &num_bits);
  ReadRaw(in, &num_hashes);
  ReadRaw(in, &byte_len);
  std::vector<uint8_t> bits(byte_len);
  if (byte_len > 0) {
    in.read(reinterpret_cast<char*>(bits.data()), byte_len);
  }
  bloom_filter_ = BloomFilter(std::move(bits), num_hashes);
}

SSTableGetResult SSTable::Get(const std::string& key,
                               std::string* out_value) const {
  if (!bloom_filter_.MightContain(key)) {
    return SSTableGetResult::kNotPresent;
  }
  if (sparse_index_.empty()) {
    return SSTableGetResult::kNotPresent;
  }

  auto it = std::upper_bound(
      sparse_index_.begin(), sparse_index_.end(), key,
      [](const std::string& k, const IndexEntry& e) { return k < e.key; });

  if (it == sparse_index_.begin()) {
    return SSTableGetResult::kNotPresent;
  }
  --it;
  uint64_t start_offset = it->offset;

  std::ifstream in(path_, std::ios::binary);
  in.seekg(static_cast<std::streamoff>(start_offset), std::ios::beg);

  while (static_cast<uint64_t>(in.tellg()) < data_section_end_) {
    uint32_t key_len = 0, val_len = 0;
    uint8_t op_byte = 0;
    if (!ReadRaw(in, &key_len)) break;
    if (!ReadRaw(in, &val_len)) break;
    if (!ReadRaw(in, &op_byte)) break;

    std::string record_key(key_len, '\0');
    if (key_len > 0) in.read(&record_key[0], key_len);
    std::string record_value(val_len, '\0');
    if (val_len > 0) in.read(&record_value[0], val_len);
    if (!in) break;

    if (record_key == key) {
      if (static_cast<OpType>(op_byte) == OpType::kDelete) {
        return SSTableGetResult::kDeleted;
      }
      if (out_value != nullptr) {
        *out_value = record_value;
      }
      return SSTableGetResult::kFound;
    }
    if (record_key > key) {
      break;
    }
  }

  return SSTableGetResult::kNotPresent;
}

std::vector<MemtableEntry> SSTable::AllEntries() const {
  std::vector<MemtableEntry> result;

  std::ifstream in(path_, std::ios::binary);
  in.seekg(0, std::ios::beg);

  while (static_cast<uint64_t>(in.tellg()) < data_section_end_) {
    uint32_t key_len = 0, val_len = 0;
    uint8_t op_byte = 0;
    if (!ReadRaw(in, &key_len)) break;
    if (!ReadRaw(in, &val_len)) break;
    if (!ReadRaw(in, &op_byte)) break;

    std::string key(key_len, '\0');
    if (key_len > 0) in.read(&key[0], key_len);
    std::string value(val_len, '\0');
    if (val_len > 0) in.read(&value[0], val_len);
    if (!in) break;

    result.push_back(
        MemtableEntry{std::move(key), std::move(value),
                       static_cast<OpType>(op_byte)});
  }

  return result;
}
```

---

## 6. Full Compaction Code

### `src/storage/compaction.h`

```cpp
#pragma once

#include <string>
#include <vector>

// Simplest possible compaction strategy (design doc §1.6, step 6):
// merge every given SSTable into one new file. This is the "merge-all"
// scheme, not leveled compaction — see the accompanying doc for why
// tombstones can only be safely dropped unconditionally in this scheme,
// not once real leveled compaction exists.
class Compactor {
 public:
  // Merges `input_paths`, given OLDEST FIRST, into a single new SSTable
  // at `output_path`. For each key, keeps only the version from the
  // newest input file that contains it; tombstones for keys whose
  // newest version is a delete are dropped from the output entirely,
  // since this merge consumes every known SSTable (see section 4).
  //
  // If `delete_inputs_on_success` is true and the merge succeeds, the
  // original input files are removed from disk — only do this once
  // you're confident output_path is durably written (matches §1.3's
  // "delete WAL only once flush is durable" caution, same principle
  // applied to compaction).
  //
  // Returns false (and leaves all inputs untouched) if the merge or
  // write fails for any reason.
  static bool CompactAll(const std::vector<std::string>& input_paths,
                          const std::string& output_path,
                          bool delete_inputs_on_success = false);

  // Simple trigger check: true once `current_file_count` has crossed
  // `threshold`. Deliberately trivial — the interesting logic lives in
  // CompactAll, not in deciding when to call it.
  static bool ShouldCompact(size_t current_file_count, size_t threshold);
};
```

### `src/storage/compaction.cpp`

```cpp
#include "storage/compaction.h"

#include <algorithm>
#include <cstdio>
#include <unordered_map>

#include "storage/sstable.h"

bool Compactor::ShouldCompact(size_t current_file_count, size_t threshold) {
  return current_file_count > threshold;
}

bool Compactor::CompactAll(const std::vector<std::string>& input_paths,
                            const std::string& output_path,
                            bool delete_inputs_on_success) {
  // Resolve every key to its newest version by reading input files in
  // oldest-to-newest order and letting later files overwrite earlier
  // ones in the map — last write wins, matching "newest version wins"
  // from section 3.
  std::unordered_map<std::string, MemtableEntry> latest;

  for (const auto& path : input_paths) {
    SSTable sst(path);
    for (auto& entry : sst.AllEntries()) {
      latest[entry.key] = std::move(entry);
    }
  }

  // Build the merged, sorted output — dropping tombstones per section 4:
  // this merge covers every known SSTable, so a key whose newest version
  // is a delete has no older file left that could resurrect it.
  std::vector<MemtableEntry> merged;
  merged.reserve(latest.size());
  for (auto& [key, entry] : latest) {
    if (entry.op_type == OpType::kDelete) {
      continue;
    }
    merged.push_back(std::move(entry));
  }

  std::sort(merged.begin(), merged.end(),
            [](const MemtableEntry& a, const MemtableEntry& b) {
              return a.key < b.key;
            });

  if (!SSTable::WriteFromEntries(output_path, merged)) {
    return false;
  }

  if (delete_inputs_on_success) {
    for (const auto& path : input_paths) {
      std::remove(path.c_str());
    }
  }

  return true;
}
```

### `test/unit/storage/test_compaction.cpp`

```cpp
#include <cassert>
#include <cstdio>
#include <iostream>
#include <vector>
#include "storage/compaction.h"
#include "storage/sstable.h"

namespace {
const char* kFile1 = "test_compaction_1.sst";
const char* kFile2 = "test_compaction_2.sst";
const char* kFile3 = "test_compaction_3.sst";
const char* kOutput = "test_compaction_output.sst";

void Cleanup() {
  std::remove(kFile1);
  std::remove(kFile2);
  std::remove(kFile3);
  std::remove(kOutput);
}
}  // namespace

int main() {
  Cleanup();

  // --- Disjoint keys: everything should just show up in the output ---
  {
    SSTable::WriteFromEntries(
        kFile1, {MemtableEntry{"apple", "red", OpType::kPut}});
    SSTable::WriteFromEntries(
        kFile2, {MemtableEntry{"banana", "yellow", OpType::kPut}});

    assert(Compactor::CompactAll({kFile1, kFile2}, kOutput));

    SSTable out(kOutput);
    std::string v;
    assert(out.Get("apple", &v) == SSTableGetResult::kFound && v == "red");
    assert(out.Get("banana", &v) == SSTableGetResult::kFound &&
           v == "yellow");
  }

  // --- Overlapping key: newest file's version must win ---
  Cleanup();
  {
    // kFile1 = oldest, kFile2 = newest, per CompactAll's oldest-first contract
    SSTable::WriteFromEntries(
        kFile1, {MemtableEntry{"apple", "red", OpType::kPut}});
    SSTable::WriteFromEntries(
        kFile2, {MemtableEntry{"apple", "green", OpType::kPut}});

    assert(Compactor::CompactAll({kFile1, kFile2}, kOutput));

    SSTable out(kOutput);
    std::string v;
    assert(out.Get("apple", &v) == SSTableGetResult::kFound && v == "green");
  }

  // --- Tombstone in the newest file: key must be gone entirely from the
  //     output, not written forward as a tombstone (section 3/4) ---
  Cleanup();
  {
    SSTable::WriteFromEntries(
        kFile1, {MemtableEntry{"apple", "red", OpType::kPut},
                 MemtableEntry{"banana", "yellow", OpType::kPut}});
    SSTable::WriteFromEntries(
        kFile2, {MemtableEntry{"banana", "", OpType::kDelete}});

    assert(Compactor::CompactAll({kFile1, kFile2}, kOutput));

    SSTable out(kOutput);
    std::string v;
    assert(out.Get("apple", &v) == SSTableGetResult::kFound && v == "red");
    // Not kDeleted — the tombstone itself should be GONE, not present.
    // Both are observably "not found" from Get()'s perspective here,
    // but AllEntries() below proves it's actually absent, not a
    // preserved tombstone record.
    assert(out.Get("banana", &v) == SSTableGetResult::kNotPresent);

    auto all = out.AllEntries();
    assert(all.size() == 1);  // only "apple" — banana's tombstone was
                               // dropped from the file entirely
  }

  // --- Three-way merge, mixed Put/Delete/Put across files ---
  Cleanup();
  {
    SSTable::WriteFromEntries(
        kFile1, {MemtableEntry{"apple", "red", OpType::kPut}});
    SSTable::WriteFromEntries(
        kFile2, {MemtableEntry{"apple", "green", OpType::kPut},
                 MemtableEntry{"banana", "yellow", OpType::kPut}});
    SSTable::WriteFromEntries(
        kFile3, {MemtableEntry{"banana", "", OpType::kDelete},
                 MemtableEntry{"cherry", "dark red", OpType::kPut}});

    assert(Compactor::CompactAll({kFile1, kFile2, kFile3}, kOutput));

    SSTable out(kOutput);
    std::string v;
    assert(out.Get("apple", &v) == SSTableGetResult::kFound && v == "green");
    assert(out.Get("banana", &v) == SSTableGetResult::kNotPresent);
    assert(out.Get("cherry", &v) == SSTableGetResult::kFound &&
           v == "dark red");

    // Output must be sorted.
    auto all = out.AllEntries();
    for (size_t i = 1; i < all.size(); i++) {
      assert(all[i - 1].key < all[i].key);
    }
  }

  // --- delete_inputs_on_success actually removes the input files ---
  Cleanup();
  {
    SSTable::WriteFromEntries(
        kFile1, {MemtableEntry{"apple", "red", OpType::kPut}});
    SSTable::WriteFromEntries(
        kFile2, {MemtableEntry{"banana", "yellow", OpType::kPut}});

    assert(Compactor::CompactAll({kFile1, kFile2}, kOutput,
                                  /*delete_inputs_on_success=*/true));

    std::ifstream f1_check(kFile1);
    std::ifstream f2_check(kFile2);
    assert(!f1_check.good());
    assert(!f2_check.good());

    // Output itself must still be intact and correct.
    SSTable out(kOutput);
    std::string v;
    assert(out.Get("apple", &v) == SSTableGetResult::kFound);
    assert(out.Get("banana", &v) == SSTableGetResult::kFound);
  }

  // --- ShouldCompact trigger ---
  {
    assert(!Compactor::ShouldCompact(/*current=*/4, /*threshold=*/4));
    assert(Compactor::ShouldCompact(/*current=*/5, /*threshold=*/4));
  }

  Cleanup();
  std::cout << "All compaction tests passed.\n";
  return 0;
}
```

Note: `test_compaction.cpp` uses `std::ifstream` for the file-existence
check in the last block — add `#include <fstream>` at the top alongside
the other includes if your compiler doesn't pull it in transitively.

---

## 7. Build Setup

Full `CMakeLists.txt` — replace wholesale:

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
    src/storage/bloom_filter.cpp
    src/storage/sstable.cpp
    src/storage/compaction.cpp
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

add_executable(test_bloom_filter test/unit/storage/test_bloom_filter.cpp)
target_link_libraries(test_bloom_filter PRIVATE storage)
add_test(NAME test_bloom_filter COMMAND test_bloom_filter)

add_executable(test_sstable test/unit/storage/test_sstable.cpp)
target_link_libraries(test_sstable PRIVATE storage)
add_test(NAME test_sstable COMMAND test_sstable)

add_executable(test_compaction test/unit/storage/test_compaction.cpp)
target_link_libraries(test_compaction PRIVATE storage)
add_test(NAME test_compaction COMMAND test_compaction)
```

Build and run:

```bash
rm -rf build
cmake -B build
cmake --build build
./build/test_compaction
```

Worth also rerunning `./build/test_sstable` after this, since
`sstable.h`/`sstable.cpp` changed (new method added) — confirm nothing
regressed before moving on.

---

## 8. Things Worth Understanding, Not Just Running

- **Why load everything into an in-memory `unordered_map` instead of a
  real streaming k-way merge?** A true streaming merge (reading a
  little from each file at a time, always advancing whichever file has
  the smallest current key) is more memory-efficient at production
  scale — RocksDB does this. For this project's scale, loading
  everything into memory is dramatically simpler to get right, and the
  design doc explicitly green-lights "simplest possible" for this
  step. Worth being able to name the streaming-merge alternative if
  asked in an interview, even though you didn't build it — that's
  exactly the kind of tradeoff conversation this project is meant to
  equip you for.

- **Why does the map naturally give "newest wins" just from iteration
  order?** Because `input_paths` is documented as oldest-first, and
  `std::unordered_map`'s `operator[]` assignment always overwrites
  whatever was there before. Reading files in that guaranteed order
  means the last write into the map for any given key is always, by
  construction, the newest one. This is a case where correctness
  depends entirely on a documented precondition of the function
  (oldest-first ordering) — worth flagging in your own head as the
  kind of assumption that would be worth an assertion or a defensive
  check in a production system, even though it's just a doc comment
  here.

- **What would go wrong if `CompactAll` deleted input files *before*
  confirming the output write succeeded?** Exact same crash-safety
  shape as the WAL-deletion caution from §1.3: if the process crashed
  between deleting inputs and finishing the output write, you'd lose
  data with nothing left to reconstruct it from. That's why
  `delete_inputs_on_success` only fires after `WriteFromEntries`
  returns true — never before.

- **Does this get called from anywhere automatically yet?** No —
  `ShouldCompact` and `CompactAll` are both plain functions with no
  background thread or scheduler triggering them. That wiring belongs
  to whatever owns the list of L0 files (the TCP server layer, next
  step) — same pattern as `Memtable::IsFull()` from the memtable step:
  this class reports state and does work when asked, the calling layer
  decides when to ask.

---

## 9. Next Step (Phase 1, Step 7 — Phase 1 Complete)

Per the build order: **wire the whole storage engine behind a TCP
server** exposing GET/PUT/DELETE, reusing your existing socket +
thread-pool experience as a starting skeleton. This is the last piece
of Phase 1 — once it's done, you have a genuinely complete, crash-safe,
single-node KV store, and Phase 1's own milestone becomes testable for
real: *"a single-node KV store that survives a `kill -9` mid-write
without losing acknowledged data."*
