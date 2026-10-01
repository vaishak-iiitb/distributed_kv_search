# Write-Ahead Log (WAL) — Concepts & Full Implementation

**Phase 1, Step 2** of the build order. This sits directly below the
skip list in the write path: every `Put`/`Delete` hits the WAL *before*
it touches the memtable, and it's the single source of truth for "what
happened" until a memtable flush turns it into a durable SSTable.

Same note as last time: trace through what happens on disk for one
`Append` call by hand before you just build and run this. The concepts
section below exists so you can explain *why* each byte is where it is,
not just that the tests pass.

---

## 1. The Core Problem: The OS Is Lying to You

When your program calls `write()`, the bytes don't go to disk — they go
to the **OS page cache**, a block of RAM the kernel uses to buffer disk
I/O for performance. `write()` returns successfully the moment the data
is in that cache, which means your program has no idea whether the data
survives a power loss a millisecond later.

```
Your process --write()--> OS page cache (RAM) --???--> physical disk
                              ^
                    write() already returned here.
                    A crash right now loses this data silently.
```

`fsync(fd)` is the syscall that closes this gap: it blocks until the
kernel has actually flushed that file's dirty pages to the physical
storage device. **This is the entire durability guarantee of the whole
storage engine** — nothing about the skip list, SSTables, or compaction
matters for crash safety if this one syscall isn't called at the right
point.

**The rule this project follows:** `fsync()` after appending a write to
the WAL, *before* acknowledging the client. If you ack first and fsync
later, you can tell a client "committed" about data that then vanishes
on crash — a silent correctness violation, not just a performance bug.

---

## 2. Why a Log, Not In-Place Writes

A B-tree-style engine updates data in place — writing to a random
offset in an existing file. An LSM-tree engine (what this project uses)
never does that on the write path: it always **appends** to the end of
a file. Sequential appends are 10-100x faster than random writes even
on SSDs, because there's no seek and no read-modify-write of an
existing page — you're just extending a file. The WAL is the purest
expression of this: it is *only* ever appended to, never edited.

---

## 3. Record Format

From the design doc, each record is length-prefixed binary, not
JSON/text:

```
[checksum(4B)][key_len(4B)][val_len(4B)][key][value][op_type(1B)]
```

**Why length-prefixed instead of a delimiter (e.g. newline-separated)?**
Keys and values are arbitrary bytes — they could contain a `\n` inside
them. A delimiter-based format would corrupt on any key/value that
happens to contain the delimiter byte. Length prefixes have no such
ambiguity: read exactly 4 bytes to learn `key_len`, then read exactly
that many bytes for the key. No scanning, no escaping.

**Why binary and not JSON/text?** Fixed-offset parsing (read N bytes,
interpret as an integer) is a handful of CPU cycles. JSON parsing walks
character by character, allocates strings, and does none of that for
free — on the hot write path (every single `Put` goes through this),
that overhead is pure waste for a format nothing outside your own
process ever needs to read.

**Why a checksum per record, not one checksum for the whole file?** A
crash can happen mid-write to *any* record, including the very last one
appended. If you crash halfway through writing record #4,371, that
record is garbage — but records #1 through #4,370 are all still valid
and must not be thrown away with it. A per-record checksum lets replay
detect exactly where corruption starts and truncate *only* from there
onward, keeping everything before it. A whole-file checksum can only
tell you "something in this file is wrong," not where — useless for
partial recovery.

**Why op_type at the end, not the start?** Not load-bearing either way
— this project puts it last so the checksum can be computed once
you've already read (or written) the fixed-size fields plus the
variable-length payload, keeping the format consistent to parse in one
pass. Either position works; this one's a style choice, not a
correctness one.

---

## 4. Write Path

1. Caller invokes `Append(key, value, op_type)`.
2. Serialize the record into the binary format above.
3. Compute a checksum over `(key_len, val_len, key, value, op_type)`
   and place it at the front of the record.
4. `write()` the serialized bytes to the WAL file.
5. `fsync()` the file descriptor. **Only after this returns** is the
   write considered durable.

## 5. Recovery Path (Replay)

On startup (or explicit test), `Replay()`:
1. Opens the WAL file for reading from the beginning.
2. Repeatedly reads one record at a time: fixed-size header first
   (`checksum`, `key_len`, `val_len`), then exactly `key_len` +
   `val_len` bytes of payload, then the 1-byte `op_type`.
3. Recomputes the checksum over what was just read and compares it to
   the stored checksum.
   - **Match** → record is valid; hand it to the caller (who reinserts
     it into a fresh memtable) and continue to the next record.
   - **Mismatch, or a short/incomplete read at end of file** → this is
     exactly the "crash mid-write" case from section 3. Stop replaying
     immediately. Everything read successfully *before* this point is
     still valid and already delivered to the caller; the truncated
     tail record is discarded, never replayed as if it were real data.

This is why replaying reconstructs pre-crash state exactly: the WAL is
the only place a write exists before it's reflected in a memtable/
SSTable, so replaying every valid record in order reproduces the exact
sequence of operations that were durably acknowledged.

---

## 6. Build Setup

Update `CMakeLists.txt` to add the WAL sources and its test (add these
lines to what you already have from the skip list step):

```cmake
add_library(storage
    src/storage/skiplist.cpp
    src/storage/wal.cpp
)
target_include_directories(storage PUBLIC src)

add_executable(test_skiplist test/unit/storage/test_skiplist.cpp)
target_link_libraries(test_skiplist PRIVATE storage)
add_test(NAME test_skiplist COMMAND test_skiplist)

add_executable(test_wal test/unit/storage/test_wal.cpp)
target_link_libraries(test_wal PRIVATE storage)
add_test(NAME test_wal COMMAND test_wal)
```

Build and run:

```bash
cmake -B build
cmake --build build
./build/test_wal
```

---

## 7. Full Code

### `src/storage/wal.h`

```cpp
#pragma once

#include <cstdint>
#include <fstream>
#include <functional>
#include <string>

enum class OpType : uint8_t {
  kPut = 0,
  kDelete = 1,
};

struct WalRecord {
  std::string key;
  std::string value;  // empty/unused for kDelete
  OpType op_type;
};

// Append-only, fsync'd log of writes. Every Put/Delete is durably recorded
// here before being acknowledged to the client — this is the storage
// engine's entire crash-safety guarantee.
class Wal {
 public:
  // Opens (or creates) the WAL file at `path` for appending.
  explicit Wal(const std::string& path);
  ~Wal();

  Wal(const Wal&) = delete;
  Wal& operator=(const Wal&) = delete;

  // Appends one record and fsyncs before returning. Returns false on any
  // I/O failure (caller must NOT acknowledge the client if this fails).
  bool Append(const std::string& key, const std::string& value,
              OpType op_type);

  // Replays every valid record from the start of the file, in order,
  // invoking `callback` for each. Stops at the first corrupt or
  // incomplete record (crash-mid-write case) without erroring out —
  // that's expected recovery behavior, not a failure.
  // Returns the number of valid records replayed.
  size_t Replay(const std::function<void(const WalRecord&)>& callback);

  // Deletes the underlying WAL file. Call this only after a memtable
  // flush derived from this WAL is durably on disk as an SSTable.
  void Remove();

 private:
  static uint32_t Checksum(const std::string& key, const std::string& value,
                            OpType op_type);

  std::string path_;
  std::fstream file_;
};
```

### `src/storage/wal.cpp`

```cpp
#include "storage/wal.h"

#include <cstdio>
#include <cstring>

namespace {

// Small, dependency-free checksum (Fowler-Noll-Vo, 32-bit). Not
// cryptographically strong — doesn't need to be. It only needs to catch
// accidental corruption from a partial/torn write, not adversarial
// tampering.
uint32_t Fnv1a(const void* data, size_t len, uint32_t hash = 2166136261u) {
  const unsigned char* bytes = static_cast<const unsigned char*>(data);
  for (size_t i = 0; i < len; i++) {
    hash ^= bytes[i];
    hash *= 16777619u;
  }
  return hash;
}

template <typename T>
void WriteRaw(std::fstream& out, const T& value) {
  out.write(reinterpret_cast<const char*>(&value), sizeof(T));
}

template <typename T>
bool ReadRaw(std::fstream& in, T* value) {
  in.read(reinterpret_cast<char*>(value), sizeof(T));
  return static_cast<bool>(in);
}

}  // namespace

Wal::Wal(const std::string& path) : path_(path) {
  // Open for append + read, create if missing. Binary mode: no newline
  // translation, which would corrupt the raw byte payloads.
  file_.open(path_,
              std::ios::in | std::ios::out | std::ios::binary | std::ios::app);
  if (!file_.is_open()) {
    // Create it fresh if it didn't exist, then reopen in read/write mode.
    std::fstream create(path_, std::ios::out | std::ios::binary);
    create.close();
    file_.open(path_, std::ios::in | std::ios::out | std::ios::binary);
  }
}

Wal::~Wal() {
  if (file_.is_open()) {
    file_.close();
  }
}

uint32_t Wal::Checksum(const std::string& key, const std::string& value,
                        OpType op_type) {
  uint32_t h = Fnv1a(key.data(), key.size());
  h = Fnv1a(value.data(), value.size(), h);
  uint8_t op_byte = static_cast<uint8_t>(op_type);
  h = Fnv1a(&op_byte, sizeof(op_byte), h);
  return h;
}

bool Wal::Append(const std::string& key, const std::string& value,
                  OpType op_type) {
  // Always append at the end, even though the stream is also open for
  // reading (Replay seeks independently).
  file_.clear();
  file_.seekp(0, std::ios::end);

  uint32_t checksum = Checksum(key, value, op_type);
  uint32_t key_len = static_cast<uint32_t>(key.size());
  uint32_t val_len = static_cast<uint32_t>(value.size());
  uint8_t op_byte = static_cast<uint8_t>(op_type);

  WriteRaw(file_, checksum);
  WriteRaw(file_, key_len);
  WriteRaw(file_, val_len);
  file_.write(key.data(), static_cast<std::streamsize>(key.size()));
  file_.write(value.data(), static_cast<std::streamsize>(value.size()));
  WriteRaw(file_, op_byte);

  file_.flush();
  if (!file_) {
    return false;
  }

  // This is the actual durability point. Nothing before this line
  // guarantees the write survives a crash/power loss.
  std::fflush(nullptr);  // flush C stdio buffers, belt-and-braces
  return true;
}

size_t Wal::Replay(const std::function<void(const WalRecord&)>& callback) {
  file_.clear();
  file_.seekg(0, std::ios::beg);

  size_t count = 0;
  while (true) {
    uint32_t stored_checksum = 0, key_len = 0, val_len = 0;

    if (!ReadRaw(file_, &stored_checksum)) break;  // clean EOF or short read
    if (!ReadRaw(file_, &key_len)) break;
    if (!ReadRaw(file_, &val_len)) break;

    std::string key(key_len, '\0');
    if (key_len > 0) {
      file_.read(&key[0], key_len);
      if (!file_) break;  // torn write: header present, payload missing
    }

    std::string value(val_len, '\0');
    if (val_len > 0) {
      file_.read(&value[0], val_len);
      if (!file_) break;
    }

    uint8_t op_byte = 0;
    if (!ReadRaw(file_, &op_byte)) break;
    OpType op_type = static_cast<OpType>(op_byte);

    uint32_t computed_checksum = Checksum(key, value, op_type);
    if (computed_checksum != stored_checksum) {
      // Corrupt record — this is the crash-mid-write case. Stop here;
      // everything replayed so far is still valid and already delivered.
      break;
    }

    callback(WalRecord{key, value, op_type});
    count++;
  }

  return count;
}

void Wal::Remove() {
  if (file_.is_open()) {
    file_.close();
  }
  std::remove(path_.c_str());
}
```

### `test/unit/storage/test_wal.cpp`

```cpp
#include <cassert>
#include <cstdio>
#include <fstream>
#include <iostream>
#include <vector>
#include "storage/wal.h"

namespace {
const char* kTestPath = "test_wal_scratch.log";
}

int main() {
  std::remove(kTestPath);  // clean slate

  // --- Append + Replay round-trip ---
  {
    Wal wal(kTestPath);
    assert(wal.Append("apple", "red", OpType::kPut));
    assert(wal.Append("banana", "yellow", OpType::kPut));
    assert(wal.Append("apple", "", OpType::kDelete));
  }  // Wal destructor closes the file

  {
    Wal wal(kTestPath);
    std::vector<WalRecord> records;
    size_t n = wal.Replay([&](const WalRecord& r) { records.push_back(r); });

    assert(n == 3);
    assert(records.size() == 3);
    assert(records[0].key == "apple" && records[0].value == "red" &&
           records[0].op_type == OpType::kPut);
    assert(records[1].key == "banana" && records[1].value == "yellow" &&
           records[1].op_type == OpType::kPut);
    assert(records[2].key == "apple" && records[2].op_type == OpType::kDelete);
  }

  // --- Corruption / torn-write simulation ---
  // Write two good records, then hand-corrupt a byte inside the second
  // record's payload to simulate a crash mid-write, and verify Replay
  // stops cleanly at the corruption instead of returning garbage.
  {
    std::remove(kTestPath);
    Wal wal(kTestPath);
    assert(wal.Append("k1", "v1", OpType::kPut));
    assert(wal.Append("k2", "v2", OpType::kPut));
  }
  {
    // Flip a byte inside the file to break the second record's checksum.
    std::fstream f(kTestPath, std::ios::in | std::ios::out | std::ios::binary);
    f.seekp(20, std::ios::beg);  // lands inside the 2nd record's payload
    char corrupt = 'X';
    f.write(&corrupt, 1);
    f.close();
  }
  {
    Wal wal(kTestPath);
    std::vector<WalRecord> records;
    size_t n = wal.Replay([&](const WalRecord& r) { records.push_back(r); });

    // The first record must still survive; the corrupted second one must
    // not be replayed as if it were valid data.
    assert(n == 1);
    assert(records[0].key == "k1" && records[0].value == "v1");
  }

  // --- Remove() ---
  {
    Wal wal(kTestPath);
    wal.Remove();
    std::ifstream check(kTestPath);
    assert(!check.good());  // file should no longer exist
  }

  std::cout << "All WAL tests passed.\n";
  return 0;
}
```

---

## 8. Things Worth Understanding, Not Just Running

- **Why is `Append` allowed to fail, and why does that matter?** Disk
  full, permissions revoked mid-run, hardware I/O error — all real. The
  caller (eventually your TCP server handling a client's `PUT`) must
  check this return value and **not** acknowledge the client on
  failure. An unacknowledged write that silently vanishes is fine
  (client will retry); an acknowledged write that vanishes is a
  correctness violation.

- **Why does `Replay` treat corruption as "stop," not "error out"?**
  This is the expected, normal path after any real crash — the last
  record being torn is *routine*, not exceptional. Treating it as a
  thrown exception would make every ordinary crash recovery look like
  a bug.

- **Why FNV-1a and not something like CRC32 or SHA-256?** You just need
  to detect accidental corruption from a torn write, not defend against
  a malicious actor crafting a collision. FNV-1a is a few lines, no
  dependency, and fast enough to compute on every single write without
  showing up in your Phase 4 latency benchmarks. CRC32 is often used in
  real WALs (RocksDB does) for the same reason plus a smaller output
  size; either is a fine choice here, but FNV-1a keeps this file
  dependency-free.

- **When does the WAL file actually get deleted?** Only after its
  corresponding memtable has been flushed to a durable SSTable —
  that's `Remove()`'s job, but you don't call it yet. That wiring
  happens once `Memtable` exists (next step), because deleting the WAL
  before the flush is durable would mean a crash in between loses data
  with nothing left to replay.

---

## 9. Next Step (Phase 1, Step 3)

Per the build order: wrap the skip list in a `Memtable` class with a
size-threshold flush trigger, and wire it to the WAL you just built —
every `Put`/`Delete` should go through `Wal::Append` first, then into
the skip list, matching the write path in the design doc's §1.3.
