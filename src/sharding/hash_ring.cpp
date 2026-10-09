#include "sharding/hash_ring.h"

#include <algorithm>
#include <string>

namespace sharding {

uint64_t Fnv1a64(std::string_view data) {
  uint64_t h = 1469598103934665603ULL;
  for (unsigned char c : data) {
    h ^= c;
    h *= 1099511628211ULL;
  }
  return h;
}

uint64_t HashKey(std::string_view key) {
  uint64_t k = Fnv1a64(key);
  // murmur3 fmix64: spreads FNV's weak low-entropy differences across all bits.
  k ^= k >> 33;
  k *= 0xff51afd7ed558ccdULL;
  k ^= k >> 33;
  k *= 0xc4ceb9fe1a85ec53ULL;
  k ^= k >> 33;
  return k;
}

HashRing::HashRing(uint32_t vnodes_per_shard) : vnodes_(vnodes_per_shard) {}

bool HashRing::AddShard(ShardId id) {
  auto it = std::lower_bound(shards_.begin(), shards_.end(), id);
  if (it != shards_.end() && *it == id) return false;
  shards_.insert(it, id);
  Rebuild();
  return true;
}

bool HashRing::RemoveShard(ShardId id) {
  auto it = std::lower_bound(shards_.begin(), shards_.end(), id);
  if (it == shards_.end() || *it != id) return false;
  shards_.erase(it);
  Rebuild();
  return true;
}

bool HashRing::HasShard(ShardId id) const {
  return std::binary_search(shards_.begin(), shards_.end(), id);
}

void HashRing::Rebuild() {
  // Regenerated from scratch from the shard SET, so the result is independent
  // of the order in which shards were added/removed.
  points_.clear();
  points_.reserve(shards_.size() * vnodes_);
  for (ShardId s : shards_) {
    for (uint32_t i = 0; i < vnodes_; ++i) {
      std::string label = "shard-" + std::to_string(s) + "#" + std::to_string(i);
      points_.push_back({HashKey(label), s});
    }
  }
  std::sort(points_.begin(), points_.end(), [](const Point& a, const Point& b) {
    return a.pos != b.pos ? a.pos < b.pos : a.shard < b.shard;
  });
  // Collision rule: if two points share a position, the lower ShardId wins.
  points_.erase(std::unique(points_.begin(), points_.end(),
                            [](const Point& a, const Point& b) { return a.pos == b.pos; }),
                points_.end());
}

std::optional<ShardId> HashRing::Lookup(std::string_view key) const {
  if (points_.empty()) return std::nullopt;
  uint64_t h = HashKey(key);
  auto it = std::lower_bound(points_.begin(), points_.end(), h,
                             [](const Point& p, uint64_t v) { return p.pos < v; });
  if (it == points_.end()) it = points_.begin();  // wrap around
  return it->shard;
}

}  // namespace sharding