# SSTable — Concepts & Full Implementation

**Phase 1, Step 4** of the build order (design doc §1.6). This is the
piece that finally lets a frozen `Memtable` leave RAM and become a
durable, immutable file on disk — composing everything you've built so
far: the bloom filter guards reads, the record format echoes the WAL's
length-prefixed approach, and the whole point of this file is to
implement the read/write paths from design doc §1.2–§1.4.

Bigger piece than the last four, so trace through one full write
(`WriteFromEntries`) and one full read (`Get`) on paper with a tiny
4-5 key example — sketch out what byte offsets everything lands at —
before running anything.

---

## 1. What Problem This Solves

Per §1.3's write path: once a memtable crosses its size threshold, it
gets frozen and flushed to a new **L0 SSTable**. An SSTable is:

- **Immutable** — once written, never modified. No in-place updates,
  ever. This is the LSM-tree philosophy carried all the way through:
  writes only ever *create new files*; compaction (next step) is what
  eventually merges and discards old ones.
- **Sorted** — keys appear in ascending order, which is what makes
  binary search over the sparse index possible.
- **Self-contained** — one file holds everything needed to answer
  reads against it: the actual data, a sparse index for fast lookup,
  and a bloom filter to skip the file entirely when possible.

## 2. File Layout

From design doc §1.2, and what this implementation builds exactly:

```
[data record 0][data record 1]...[data record N]     <- sorted by key
[sparse index: count, then (key, offset) pairs]
[bloom filter: num_bits, num_hashes, byte_len, bitmap bytes]
[footer: index_offset(8B), bloom_offset(8B), magic_number(4B)]
```

**One simplification worth being upfront about**: the design doc
describes "data blocks" as fixed-size chunks (e.g. 4KB) that you scan
linearly once you've binary-searched into the right one. This
implementation treats the data section as a flat sequence of
variable-length records instead, and indexes **every Nth record**
(not every Nth 4KB-aligned block). The read algorithm is identical in
spirit — binary search the sparse index to find where to start, then
scan forward linearly — just without the extra complexity of aligning
records to fixed byte boundaries. This is a legitimate simplification
for a project at this scale, and worth mentioning as a deliberate
choice if asked: real systems block-align for I/O efficiency (reading
a whole 4KB page from disk is cheap regardless of how much of it you
use), which matters at production scale but isn't the thing this
project is being built to prove out.

**Record format** (each entry in the data section):
```
[key_len(4B)][val_len(4B)][op_type(1B)][key][value]
```
Same length-prefixed philosophy as the WAL, for the same reasons
(section 3 of the WAL doc): fixed-offset parsing, no delimiter
ambiguity.

**Why the footer is fixed-size and at the very end**: to read *any*
part of this file, you need to know where the index and bloom filter
sections start — but you don't know that until you've written the
whole data section, since its length varies. Writing the footer last,
at a fixed 20-byte size, means a reader can always `seek(-20, END)`
first, read the footer, and from there know exactly where everything
else in the file lives — without having to scan the file front to
back just to find the index.

---

## 3. Write Path

Given a sorted list of entries (from a frozen memtable):

1. For each entry, in order: if it's every `kIndexInterval`-th record
   (16 in this implementation), record `(key, current_offset)` into
   the sparse index. Add the key to a bloom filter sized for the
   total entry count. Write the record.
2. After all records are written, write the sparse index section:
   entry count, then each `(key, offset)` pair.
3. Write the bloom filter section: its bit count, hash count, and the
   raw bitmap bytes (this is exactly `BloomFilter::Bits()` and
   `NumHashes()` from the last step, serialized).
4. Write the footer: where the index section started, where the bloom
   section started, and a magic number (a fixed sentinel value used to
   sanity-check "is this actually an SSTable file" — not load-bearing
   for correctness here, but standard practice and cheap insurance
   against silently reading a corrupt/wrong file as if it were valid).

## 4. Read Path

This is design doc §1.4's per-file check, applied to one SSTable:

1. **Bloom filter check first.** If `MightContain(key)` is false, stop
   immediately — this key is definitely not in this file. No disk
   seek into the data section at all.
2. **Binary search the sparse index** for the last indexed key that is
   `<=` the target key. That gives you a byte offset to start
   scanning from — somewhere at or before where the target key would
   be, if present.
3. **Linear scan forward** from that offset, reading one record at a
   time:
   - Found the target key → check its `op_type`. `kDelete` → return
     "deleted" (tombstone). `kPut` → return the value.
   - Read a key that sorts *after* the target → stop scanning; the
     target isn't in this file (since records are sorted, once you've
     passed where it would be, it's not coming).
   - Reach the end of the data section → stop; not found.

Steps 2-3 together mean you only ever read a small slice of the file
(from the nearest indexed offset up to at most `kIndexInterval`
records past it), never the whole thing — same complexity trade the
sparse index exists for in the first place.

---

## 5. Updated `Memtable` — Adding `AllSorted()`

SSTable's writer needs a sorted, decoded view of a memtable's entries
(key, value, and whether each is a tombstone) — the memtable stored
these internally as skip-list values tagged with a 1-byte `op_type`
prefix (from the memtable step), which nothing outside `Memtable`
should need to know about. So `Memtable` gets one new method that
decodes and exposes that cleanly.

**This replaces your existing `memtable.h` and `memtable.cpp`
wholesale** — the only change is the addition of `MemtableEntry` and
`AllSorted()`; everything else is identical to what you already have
and already tested.

### `src/storage/memtable.h`

```cpp
#pragma once

#include <cstddef>
#include <string>
#include <vector>

#include "storage/skiplist.h"
#include "storage/wal.h"

enum class GetResult {
  kFound,       // key present with a real value
  kDeleted,     // key present as a tombstone — definitely deleted
  kNotPresent,  // key not in this memtable at all; caller should keep
                // checking older memtables/SSTables
};

// One decoded entry, used when flushing a memtable to an SSTable.
struct MemtableEntry {
  std::string key;
  std::string value;  // empty/unused when op_type == kDelete
  OpType op_type;
};

// Wraps a skip list with write-ahead logging and a size-threshold flush
// trigger. This is the in-memory half of the LSM-tree write path from
// design doc §1.3, steps 1-3. Step 4 (actual flush to SSTable) happens
// one level up, using AllSorted() below.
class Memtable {
 public:
  Memtable(const std::string& wal_path, size_t size_threshold_bytes);

  Memtable(const Memtable&) = delete;
  Memtable& operator=(const Memtable&) = delete;

  bool Put(const std::string& key, const std::string& value);
  bool Delete(const std::string& key);
  GetResult Get(const std::string& key, std::string* out_value) const;
  bool IsFull() const;
  size_t ApproximateSize() const { return approximate_size_; }
  size_t RecoverFromWal();
  Wal& GetWal() { return wal_; }

  // Returns every entry in this memtable, in sorted key order, decoded
  // (op_type separated back out from the tagged skip-list value). Used
  // by SSTable::WriteFromEntries when flushing a frozen memtable.
  std::vector<MemtableEntry> AllSorted() const;

 private:
  static constexpr size_t kPerRecordOverhead = 32;

  void ApplyToSkipList(const std::string& key, const std::string& value,
                        OpType op_type);

  // Splits a tagged skip-list value (1-byte op_type prefix + payload)
  // back into its op_type and value. Shared by Get() and AllSorted() so
  // the decoding logic lives in exactly one place.
  static void Decode(const std::string& tagged_value, OpType* op_type,
                      std::string* value);

  SkipList skiplist_;
  Wal wal_;
  size_t size_threshold_bytes_;
  size_t approximate_size_;

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
  std::string tagged_value;
  tagged_value.reserve(value.size() + 1);
  tagged_value.push_back(static_cast<char>(op_type));
  tagged_value += value;

  skiplist_.Insert(key, tagged_value);
  approximate_size_ += key.size() + value.size() + kPerRecordOverhead;
}

void Memtable::Decode(const std::string& tagged_value, OpType* op_type,
                       std::string* value) {
  *op_type = static_cast<OpType>(tagged_value[0]);
  *value = tagged_value.substr(1);
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

  OpType op_type;
  std::string value;
  Decode(tagged_value, &op_type, &value);

  if (op_type == OpType::kDelete) {
    return GetResult::kDeleted;
  }
  if (out_value != nullptr) {
    *out_value = value;
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

std::vector<MemtableEntry> Memtable::AllSorted() const {
  std::vector<MemtableEntry> result;
  for (const auto& key : skiplist_.SortedKeys()) {
    std::string tagged_value;
    skiplist_.Search(key, &tagged_value);  // guaranteed present — key just
                                            // came from SortedKeys()

    OpType op_type;
    std::string value;
    Decode(tagged_value, &op_type, &value);

    result.push_back(MemtableEntry{key, value, op_type});
  }
  return result;
}
```

---

## 6. Build Setup

Full `CMakeLists.txt` — replace your existing file wholesale:

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
```

Build and run:

```bash
rm -rf build
cmake -B build
cmake --build build
./build/test_sstable
```

---

## 7. Full Code

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

// An immutable, sorted, on-disk file — one L0 SSTable. Layout is:
//   data records (sorted) | sparse index | bloom filter | footer
// See sections 2-4 of the accompanying doc for the exact format and the
// write/read algorithms.
class SSTable {
 public:
  // Writes a new SSTable file at `path` from entries that are ALREADY
  // sorted by key ascending (Memtable::AllSorted() guarantees this).
  // Returns false on any I/O failure.
  static bool WriteFromEntries(const std::string& path,
                                const std::vector<MemtableEntry>& entries);

  // Opens an existing SSTable file, loading its sparse index and bloom
  // filter into memory (the data section itself stays on disk and is
  // only read from during Get()).
  explicit SSTable(const std::string& path);

  SSTableGetResult Get(const std::string& key, std::string* out_value) const;

  const std::string& Path() const { return path_; }

 private:
  struct IndexEntry {
    std::string key;
    uint64_t offset;
  };

  // Every kIndexInterval-th record gets a sparse index entry. Smaller =
  // faster lookups, more index memory; larger = the opposite. 16 is a
  // reasonable default at this project's scale.
  static constexpr int kIndexInterval = 16;
  static constexpr uint32_t kMagicNumber = 0x53535442;  // ASCII "SSTB"

  std::string path_;
  std::vector<IndexEntry> sparse_index_;
  BloomFilter bloom_filter_;
  uint64_t data_section_end_;  // == index section's start offset
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
  // BloomFilter has no default constructor, so size it up front for the
  // known entry count (falls back to 1 to avoid a degenerate empty filter).
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

  // --- sparse index section ---
  uint32_t index_count = static_cast<uint32_t>(index.size());
  WriteRaw(out, index_count);
  for (const auto& entry : index) {
    WriteLengthPrefixedString(out, entry.key);
    WriteRaw(out, entry.offset);
  }

  // --- bloom filter section ---
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

  // --- footer (fixed 20 bytes, always at the very end) ---
  WriteRaw(out, index_offset);
  WriteRaw(out, bloom_offset);
  WriteRaw(out, kMagicNumber);

  out.flush();
  return static_cast<bool>(out);
}

SSTable::SSTable(const std::string& path)
    : path_(path), bloom_filter_(std::vector<uint8_t>{1}, 1) {
  // bloom_filter_ above is a throwaway placeholder — BloomFilter has no
  // default constructor, so it must be initialized in the init list, then
  // properly rebuilt below once we've actually read the file's real filter.

  std::ifstream in(path_, std::ios::binary);

  in.seekg(-20, std::ios::end);
  uint64_t index_offset = 0, bloom_offset = 0;
  uint32_t magic = 0;
  ReadRaw(in, &index_offset);
  ReadRaw(in, &bloom_offset);
  ReadRaw(in, &magic);
  // magic is intentionally not hard-asserted here beyond this comment —
  // a mismatch would mean "this isn't a valid SSTable file at all," worth
  // surfacing as a real error path once this is wired into a server; for
  // now this class trusts files it's given, matching this phase's scope.

  data_section_end_ = index_offset;

  // --- load sparse index into memory ---
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

  // --- load bloom filter into memory ---
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

  // Find the last sparse index entry whose key is <= target: the record
  // we want, if present, is at or after this entry's offset.
  auto it = std::upper_bound(
      sparse_index_.begin(), sparse_index_.end(), key,
      [](const std::string& k, const IndexEntry& e) { return k < e.key; });

  if (it == sparse_index_.begin()) {
    // Target is smaller than the smallest indexed key, which is always
    // the very first record in the file — so it can't be present at all.
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
      break;  // sorted data — passed where the key would be
    }
  }

  return SSTableGetResult::kNotPresent;
}
```

### `test/unit/storage/test_sstable.cpp`

```cpp
#include <cassert>
#include <cstdio>
#include <iostream>
#include <string>
#include <vector>
#include "storage/memtable.h"
#include "storage/sstable.h"

namespace {
const char* kSSTablePath = "test_sstable_scratch.sst";
const char* kWalPath = "test_sstable_wal_scratch.log";
}

int main() {
  std::remove(kSSTablePath);

  // --- Basic write + read round-trip, spanning multiple sparse index
  //     entries (kIndexInterval == 16, so use enough keys to exercise
  //     more than one index bucket and the linear-scan-forward path) ---
  {
    std::vector<MemtableEntry> entries;
    for (int i = 0; i < 50; i++) {
      char buf[16];
      std::snprintf(buf, sizeof(buf), "key%03d", i);  // zero-padded: sorts
                                                        // correctly as strings
      entries.push_back(
          MemtableEntry{buf, "value" + std::to_string(i), OpType::kPut});
    }

    assert(SSTable::WriteFromEntries(kSSTablePath, entries));

    SSTable sst(kSSTablePath);
    std::string v;

    // Found: first, middle, last — exercises different sparse index buckets
    assert(sst.Get("key000", &v) == SSTableGetResult::kFound &&
           v == "value0");
    assert(sst.Get("key025", &v) == SSTableGetResult::kFound &&
           v == "value25");
    assert(sst.Get("key049", &v) == SSTableGetResult::kFound &&
           v == "value49");

    // Not present: before range, between existing keys, after range
    assert(sst.Get("key000_before", &v) == SSTableGetResult::kNotPresent);
    assert(sst.Get("aaa_way_before_everything", &v) ==
           SSTableGetResult::kNotPresent);
    assert(sst.Get("zzz_way_after_everything", &v) ==
           SSTableGetResult::kNotPresent);
  }

  // --- Tombstones survive the round-trip correctly ---
  std::remove(kSSTablePath);
  {
    std::vector<MemtableEntry> entries = {
        MemtableEntry{"apple", "red", OpType::kPut},
        MemtableEntry{"banana", "yellow", OpType::kPut},
        MemtableEntry{"cherry", "", OpType::kDelete},
    };
    assert(SSTable::WriteFromEntries(kSSTablePath, entries));

    SSTable sst(kSSTablePath);
    std::string v;
    assert(sst.Get("apple", &v) == SSTableGetResult::kFound && v == "red");
    assert(sst.Get("cherry", &v) == SSTableGetResult::kDeleted);
    assert(sst.Get("durian", &v) == SSTableGetResult::kNotPresent);
  }

  // --- Empty SSTable (edge case: memtable flushed with zero entries) ---
  std::remove(kSSTablePath);
  {
    std::vector<MemtableEntry> empty;
    assert(SSTable::WriteFromEntries(kSSTablePath, empty));

    SSTable sst(kSSTablePath);
    std::string v;
    assert(sst.Get("anything", &v) == SSTableGetResult::kNotPresent);
  }

  // --- Integration: real Memtable -> AllSorted() -> SSTable, end to end ---
  std::remove(kSSTablePath);
  std::remove(kWalPath);
  {
    Memtable mt(kWalPath, /*size_threshold_bytes=*/1 << 20);
    assert(mt.Put("apple", "red"));
    assert(mt.Put("banana", "yellow"));
    assert(mt.Put("cherry", "dark red"));
    assert(mt.Delete("banana"));  // deleted after being written — tombstone
    assert(mt.Put("date", "brown"));

    auto entries = mt.AllSorted();
    // Sorted order check
    assert(entries[0].key == "apple");
    assert(entries[1].key == "banana");
    assert(entries[2].key == "cherry");
    assert(entries[3].key == "date");

    assert(SSTable::WriteFromEntries(kSSTablePath, entries));

    SSTable sst(kSSTablePath);
    std::string v;
    assert(sst.Get("apple", &v) == SSTableGetResult::kFound && v == "red");
    assert(sst.Get("banana", &v) == SSTableGetResult::kDeleted);
    assert(sst.Get("cherry", &v) == SSTableGetResult::kFound &&
           v == "dark red");
    assert(sst.Get("date", &v) == SSTableGetResult::kFound && v == "brown");
    assert(sst.Get("never_written", &v) == SSTableGetResult::kNotPresent);
  }

  std::cout << "All SSTable tests passed.\n";
  return 0;
}
```

---

## 8. Things Worth Understanding, Not Just Running

- **Why does `SSTable`'s constructor initialize `bloom_filter_` with a
  throwaway placeholder, then overwrite it?** `BloomFilter` has no
  default constructor by design (section 1 of the bloom filter doc —
  every filter needs a real size/hash-count from the moment it
  exists). But you can't read the *real* bloom filter bytes out of the
  file until you've already parsed the footer and the index section
  first. C++ requires every member to be initialized in the
  constructor's init list before the constructor body runs, so this
  placeholder-then-reassign pattern is the standard way to handle
  "I can't know the real value until some computation happens in the
  body." Not a bug — a real constraint of the language, worth being
  able to explain if asked why it's there.

- **Why must entries passed to `WriteFromEntries` already be sorted,
  rather than sorting them inside the function?** Because they always
  are already — `Memtable::AllSorted()` gets its order for free by
  walking the skip list's level-0 chain, which is already maintained
  in sorted order by construction. Re-sorting inside `SSTable` would
  be redundant O(n log n) work for data that's already O(n) to
  produce in order. This is a good example of a correctness
  *precondition* rather than something the function needs to enforce
  itself — worth documenting clearly (as the header comment does)
  rather than silently trusting.

- **Why does the read loop check `record_key > key` and break, instead
  of scanning to the end of the file every time?** This is exactly
  what bounds the cost of a miss. Without sorted data, "not found"
  would require scanning the *entire* file every time. Because the
  data is sorted, the moment you've read a key that sorts after your
  target, you've mathematically proven the target isn't anywhere
  later in the file either — no more reading needed.

- **What happens to an SSTable's bloom filter and sparse index across
  process restarts?** They're always reloaded from the file itself on
  `SSTable` construction — nothing about them is cached anywhere else.
  This is intentional: the file is the single source of truth, and
  keeping the in-memory index/filter as a pure function of "what's in
  this file" means there's no separate cache-invalidation problem to
  get wrong.

- **This SSTable never gets deleted or modified once written — so
  what happens as more and more memtables flush?** Exactly the
  question compaction (next step) exists to answer. Right now, a
  `Get` against "the whole database" would need to check *every* L0
  SSTable that's ever been flushed, oldest data eventually needing to
  be checked through an unbounded number of files. Compaction is what
  merges these down and bounds that number.

---

## 9. Next Step (Phase 1, Step 6)

Per the build order: **compaction** — start with the simplest possible
version (merge all L0 files into one when the L0 file count exceeds
some threshold `N`), deferring proper leveled compaction logic until
basic correctness is proven. This is what keeps the "how many SSTables
does a single `Get` need to check" number bounded over time, instead
of growing forever as the system runs.
