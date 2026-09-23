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