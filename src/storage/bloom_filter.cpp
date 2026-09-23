#include "storage/bloom_filter.h"

#include <cmath>

namespace {

uint32_t Fnv1a(const void* data, size_t len, uint32_t hash = 2166136261u) {
  const unsigned char* bytes = static_cast<const unsigned char*>(data);
  for (size_t i = 0; i < len; i++) {
    hash ^= bytes[i];
    hash *= 16777619u;
  }
  return hash;
}

uint32_t SecondHash(const void* data, size_t len) {
  return Fnv1a(data, len, /*seed=*/0x9E3779B9u);
}

}  // namespace

BloomFilter::BloomFilter(size_t expected_keys, int bits_per_key) {
  if (expected_keys == 0) expected_keys = 1;

  num_bits_ = expected_keys * static_cast<size_t>(bits_per_key);
  if (num_bits_ < 64) num_bits_ = 64;

  // Round up to a full byte boundary NOW, so num_bits_ always exactly
  // equals the packed bit array's size in bits. This is what makes the
  // reload constructor's recomputation (bits.size() * 8) always agree
  // with the original — without this, any expected_keys/bits_per_key
  // combination that isn't already a multiple of 8 silently corrupts
  // every hash position after a disk round-trip (see the SSTable doc's
  // section 8 for what this looked like when it broke).
  num_bits_ = ((num_bits_ + 7) / 8) * 8;

  num_hashes_ = static_cast<int>(
      std::round(static_cast<double>(bits_per_key) * 0.6931471805599453));
  if (num_hashes_ < 1) num_hashes_ = 1;
  if (num_hashes_ > 30) num_hashes_ = 30;

  bits_.assign(num_bits_ / 8, 0);
}

BloomFilter::BloomFilter(std::vector<uint8_t> bits, int num_hashes)
    : num_bits_(bits.size() * 8), num_hashes_(num_hashes), bits_(std::move(bits)) {}

void BloomFilter::HashKey(const std::string& key, uint32_t* h1, uint32_t* h2) {
  *h1 = Fnv1a(key.data(), key.size());
  *h2 = SecondHash(key.data(), key.size());
  if (*h2 == 0) *h2 = 1;
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
      return false;
    }
  }
  return true;
}