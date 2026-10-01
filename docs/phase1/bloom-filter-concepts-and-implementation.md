# Bloom Filter — Concepts & Full Implementation

**Phase 1, Step 5** of the build order (per design doc §1.6): *"Bloom
filter (standalone, unit tested independently — easy to get subtly
wrong)."* Building this before SSTable is deliberate — you want this
piece proven correct in isolation before it's buried inside a bigger
file format where a bug is much harder to localize.

Same habit as the last three: trace through one `Add` and one
`MightContain` by hand on a tiny example (map a couple of keys to bit
positions on paper) before you just build and run it.

---

## 1. The Problem This Solves

Per the design doc's read path (§1.4): a `Get` on an SSTable-backed
store may need to check **several SSTable files** before finding a key
or concluding it doesn't exist — newest to oldest. Most of the time,
a random lookup key isn't in most of those files at all. Checking "is
this key in this file" the naive way means a disk seek + read to find
out the answer is "no" — wasted I/O, over and over, for the common
case.

A **bloom filter** is a small, in-memory bitmap attached to each
SSTable that answers one question fast, with no disk access:

> "Is this key **definitely not** in this file, or **maybe** in this
> file?"

- **"Definitely not"** → skip the file entirely, save a disk seek.
- **"Maybe"** → you still have to actually check the file (sparse
  index + block scan), because bloom filters can have **false
  positives** (say "maybe" when the key isn't actually there) — but
  they can **never** have **false negatives** (say "definitely not"
  when the key actually is there). That asymmetry is what makes them
  safe to use for this optimization: they can waste a disk read
  occasionally, but they can never cause you to wrongly skip a file
  that actually had the key.

---

## 2. How It Works

A bloom filter is:
1. A bit array of size `m`, all initialized to 0.
2. `k` independent hash functions, each mapping a key to a position in
   `[0, m)`.

**Adding a key**: compute all `k` hash positions for the key, set each
of those bits to 1.

**Checking a key**: compute the same `k` hash positions, check if
*all* of them are set to 1.
- If **any** bit is 0 → the key was **definitely never added** (if it
  had been, that bit would have been set when it was added).
- If **all** bits are 1 → the key was **probably added** — but another
  key (or combination of keys) might have happened to set all those
  same bits by coincidence, without this exact key ever being added.
  That coincidence is the false positive.

```
Bit array (m=16), after adding "apple" and "banana" with k=2 hashes each:

index:  0  1  2  3  4  5  6  7  8  9  10 11 12 13 14 15
bit:    0  1  0  0  1  0  0  1  0  0  0  1  0  0  0  0
              ^apple hash1         ^apple hash2
        ^banana hash1                          (etc.)

Checking "cherry" (never added): hashes land on positions 3 and 9.
Both are 0 → definitely not present. No disk read needed.

Checking "durian" (never added): hashes happen to land on positions
1 and 7 — both already 1 from apple/banana. False positive: filter
says "maybe," you check the file, don't find it, no harm done beyond
one wasted read.
```

**Why false negatives are structurally impossible**: a key's bits only
ever get set, never cleared, and checking only ever reads bits — there
is no code path by which a key that was genuinely added could later
have one of its bits read as 0. This is why it's safe to skip a file
entirely on a "definitely not" answer.

---

## 3. Sizing: `m` and `k`

Design doc says: *"Use ~10 bits/key for ~1% false positive rate."*
That's not an arbitrary number — it comes from the standard bloom
filter sizing formulas, given here so you can defend the constant if
asked:

- Optimal bit array size for `n` expected keys and target false
  positive rate `p`:
  `m = -(n * ln(p)) / (ln(2)^2)`
- Optimal number of hash functions given `m` and `n`:
  `k = (m / n) * ln(2)`

At `p = 0.01` (1% false positive rate), this works out to
**`m/n ≈ 9.6` bits per key**, and **`k ≈ 7`** hash functions — which
is where "~10 bits/key" comes from. This implementation takes
`bits_per_key` as a constructor parameter (default 10) and derives `k`
from it internally using the formula above, so you don't hardcode a
specific `n` up front — useful since you won't always know the exact
key count before building the filter.

---

## 4. Getting `k` Hash Functions From One Hash

You don't need `k` genuinely independent hash function implementations
— that would mean shipping 7 different hash algorithms. The standard
trick (from Kirsch & Mitzenmacher's paper on the technique) is:
compute **two** independent hashes, `h1` and `h2`, then derive all `k`
positions as:

```
position_i = (h1 + i * h2) mod m,  for i = 0 .. k-1
```

This produces `k` well-distributed positions from just two underlying
hash computations, and is what most production bloom filter
implementations actually do — including RocksDB's.

---

## 5. Build Setup

Full `CMakeLists.txt` — replace your existing file with this (adds
`bloom_filter.cpp` and `test_bloom_filter`):

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
```

Build and run:

```bash
rm -rf build
cmake -B build
cmake --build build
./build/test_bloom_filter
```

---

## 6. Full Code

### `src/storage/bloom_filter.h`

```cpp
#pragma once

#include <cstddef>
#include <cstdint>
#include <string>
#include <vector>

// A standard bloom filter: definite "not present" answers with zero false
// negatives, at the cost of occasional false positives. Used by SSTable to
// skip files that provably don't contain a given key without touching disk.
class BloomFilter {
 public:
  // `expected_keys`: how many keys you expect to add (used to size the bit
  // array up front — the filter still works if you add more or fewer, just
  // with a worse false-positive rate than targeted if you add far more).
  // `bits_per_key`: design doc default is 10, giving ~1% false positive
  // rate. Higher = fewer false positives, more memory.
  explicit BloomFilter(size_t expected_keys, int bits_per_key = 10);

  // Reconstructs a filter from a previously-serialized bitmap (used when
  // reading an SSTable's footer back off disk) plus the same k that was
  // used to build it originally.
  BloomFilter(std::vector<uint8_t> bits, int num_hashes);

  // Marks `key` as present. Idempotent — adding the same key twice is
  // harmless.
  void Add(const std::string& key);

  // Returns false only if `key` was DEFINITELY never added (safe to skip
  // this file entirely). Returns true if `key` was PROBABLY added — the
  // caller must still check the actual data, since this can be a false
  // positive.
  bool MightContain(const std::string& key) const;

  // For serializing into an SSTable's footer.
  const std::vector<uint8_t>& Bits() const { return bits_; }
  int NumHashes() const { return num_hashes_; }
  size_t NumBits() const { return num_bits_; }

 private:
  // Two independent hashes of `key`, used as the basis for deriving all
  // k hash positions per section 4.
  static void HashKey(const std::string& key, uint32_t* h1, uint32_t* h2);

  void SetBit(size_t index);
  bool GetBit(size_t index) const;

  size_t num_bits_;
  int num_hashes_;
  std::vector<uint8_t> bits_;  // packed, 8 bits per byte
};
```

### `src/storage/bloom_filter.cpp`

```cpp
#include "storage/bloom_filter.h"

#include <cmath>

namespace {

// Same FNV-1a used in the WAL — good enough distribution for this purpose,
// no external dependency.
uint32_t Fnv1a(const void* data, size_t len, uint32_t hash = 2166136261u) {
  const unsigned char* bytes = static_cast<const unsigned char*>(data);
  for (size_t i = 0; i < len; i++) {
    hash ^= bytes[i];
    hash *= 16777619u;
  }
  return hash;
}

// A second, independent-enough hash: FNV-1a with a different seed produces
// a sufficiently decorrelated second hash for the Kirsch-Mitzenmacher
// technique in section 4 without needing a second real algorithm.
uint32_t SecondHash(const void* data, size_t len) {
  return Fnv1a(data, len, /*seed=*/0x9E3779B9u);
}

}  // namespace

BloomFilter::BloomFilter(size_t expected_keys, int bits_per_key) {
  if (expected_keys == 0) expected_keys = 1;  // avoid degenerate zero-size filter

  num_bits_ = expected_keys * static_cast<size_t>(bits_per_key);
  if (num_bits_ < 64) num_bits_ = 64;  // floor, so tiny filters still work

  // k = (m/n) * ln(2), per section 3's sizing formula.
  num_hashes_ = static_cast<int>(
      std::round(static_cast<double>(bits_per_key) * 0.6931471805599453));
  if (num_hashes_ < 1) num_hashes_ = 1;
  if (num_hashes_ > 30) num_hashes_ = 30;  // sanity cap

  bits_.assign((num_bits_ + 7) / 8, 0);
}

BloomFilter::BloomFilter(std::vector<uint8_t> bits, int num_hashes)
    : num_bits_(bits.size() * 8), num_hashes_(num_hashes), bits_(std::move(bits)) {}

void BloomFilter::HashKey(const std::string& key, uint32_t* h1, uint32_t* h2) {
  *h1 = Fnv1a(key.data(), key.size());
  *h2 = SecondHash(key.data(), key.size());
  if (*h2 == 0) *h2 = 1;  // avoid degenerate case where every position
                          // collapses to h1 when h2 is 0
}

void BloomFilter::SetBit(size_t index) {
  bits_[index / 8] |= static_cast<uint8_t>(1u << (index % 8));
}

bool BloomFilter::GetBit(size_t index) const {
  return (bits_[index / 8] & static_cast<uint8_t>(1u << (index % 8))) != 0;
}

void BloomFilter::Add(const std::string& key) {
  uint32_t h1, h2;
  HashKey(key, &h1, &h2);

  for (int i = 0; i < num_hashes_; i++) {
    // position_i = (h1 + i * h2) mod m, per section 4.
    uint64_t combined = static_cast<uint64_t>(h1) +
                         static_cast<uint64_t>(i) * static_cast<uint64_t>(h2);
    size_t index = static_cast<size_t>(combined % num_bits_);
    SetBit(index);
  }
}

bool BloomFilter::MightContain(const std::string& key) const {
  uint32_t h1, h2;
  HashKey(key, &h1, &h2);

  for (int i = 0; i < num_hashes_; i++) {
    uint64_t combined = static_cast<uint64_t>(h1) +
                         static_cast<uint64_t>(i) * static_cast<uint64_t>(h2);
    size_t index = static_cast<size_t>(combined % num_bits_);
    if (!GetBit(index)) {
      return false;  // definitely not present — one unset bit is proof enough
    }
  }
  return true;  // all bits set — probably present, could be a false positive
}
```

### `test/unit/storage/test_bloom_filter.cpp`

```cpp
#include <cassert>
#include <iostream>
#include <string>
#include <vector>
#include "storage/bloom_filter.h"

int main() {
  // --- No false negatives, ever: every added key must test positive ---
  {
    BloomFilter bf(/*expected_keys=*/1000);
    std::vector<std::string> keys;
    for (int i = 0; i < 1000; i++) {
      keys.push_back("key" + std::to_string(i));
    }
    for (const auto& k : keys) {
      bf.Add(k);
    }
    for (const auto& k : keys) {
      // This must NEVER fail. A false negative here would mean SSTable
      // could wrongly skip a file that actually contains the key.
      assert(bf.MightContain(k));
    }
    std::cout << "No-false-negatives check passed (1000/1000 keys found).\n";
  }

  // --- Keys never added mostly test negative (false positive rate check) ---
  {
    BloomFilter bf(/*expected_keys=*/1000, /*bits_per_key=*/10);
    for (int i = 0; i < 1000; i++) {
      bf.Add("key" + std::to_string(i));
    }

    int false_positives = 0;
    int trials = 10000;
    for (int i = 0; i < trials; i++) {
      // Keys guaranteed disjoint from the "keyN" range added above.
      std::string absent_key = "absent" + std::to_string(i);
      if (bf.MightContain(absent_key)) {
        false_positives++;
      }
    }

    double fp_rate = static_cast<double>(false_positives) / trials;
    std::cout << "Observed false positive rate: " << (fp_rate * 100)
              << "% (target ~1% at 10 bits/key)\n";

    // Loose bound — this is a probabilistic structure, not exact. Failing
    // this by a wide margin would indicate a real bug (e.g. hash functions
    // not actually independent, or sizing formula wrong), not bad luck.
    assert(fp_rate < 0.05);
  }

  // --- Empty filter: nothing should ever match ---
  {
    BloomFilter bf(/*expected_keys=*/100);
    assert(!bf.MightContain("anything"));
    assert(!bf.MightContain(""));
  }

  // --- Serialization round-trip (Bits()/NumHashes() -> reconstruct) ---
  {
    BloomFilter original(500);
    original.Add("apple");
    original.Add("banana");

    BloomFilter reconstructed(original.Bits(), original.NumHashes());
    assert(reconstructed.MightContain("apple"));
    assert(reconstructed.MightContain("banana"));
    // Still definitely-not-present for something never added (barring an
    // extremely unlucky coincidental collision, which is not expected here).
    assert(!reconstructed.MightContain("zzz_never_added_zzz"));
  }

  std::cout << "All bloom filter tests passed.\n";
  return 0;
}
```

---

## 7. Things Worth Understanding, Not Just Running

- **Why does `HashKey` guard against `h2 == 0`?** If `h2` were 0, every
  derived position in the `(h1 + i * h2) mod m` formula would collapse
  to the same value (`h1 mod m`) regardless of `i` — you'd effectively
  only be setting **one** bit per key instead of `k`, silently
  destroying the filter's accuracy without any visible error. This is
  exactly the kind of "subtly wrong" bug the design doc's warning is
  about — it wouldn't crash, wouldn't fail an obvious test, it would
  just quietly give you a much worse false-positive rate than
  expected.

- **Why is a false positive "fine" but a false negative isn't?** A
  false positive costs you one wasted disk read — you check the file,
  don't find the key, move on; SSTable's read path already handles
  "checked this file, not here, check the next one" as normal
  behavior. A false negative would mean skipping a file that
  *actually* had the key, silently returning "not found" for a key
  that exists — an actual correctness bug, not a performance one.
  That asymmetry is why the whole design leans on "never false
  negative" as an absolute guarantee, not just a low-probability
  target.

- **Why size the filter with a floor of 64 bits?** Without it, a
  filter built for a tiny number of expected keys (say, 2) would get
  `2 * 10 = 20` bits, and with `k ≈ 7` hash functions hashing into only
  20 bits, you'd saturate the filter almost immediately — every key
  would start returning false positives for nearly everything. The
  floor keeps small filters usable; it's a pragmatic guard, not part
  of the formal sizing math.

- **What happens to a bloom filter after compaction merges its
  SSTable with another?** It gets rebuilt from scratch for the new
  merged file — bloom filters aren't merged bit-by-bit in this design
  (that's a fine thing to bring up if asked in an interview: you
  *could* OR two same-sized filters together and get a valid filter
  for the union of their keys, but rebuilding fresh during compaction,
  which you're already reading every key from, is simpler and no more
  expensive here).

---

## 8. Next Step (Phase 1, Step 4, continued)

With the bloom filter now trusted in isolation, it plugs into
**SSTable** as one section of the file footer (per design doc §1.2's
layout: data blocks → sparse index → bloom filter bitmap → footer).
That's the next file — SSTable writer (from a sorted memtable) and
reader (bloom filter check → sparse index binary search → block scan),
matching the read path in §1.4.
