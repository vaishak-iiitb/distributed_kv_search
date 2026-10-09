#pragma once
// Consistent-hash ring with virtual nodes.
// The ring is a pure function of (set of shard ids, vnodes_per_shard, hash function).
#include <cstddef>
#include <cstdint>
#include <optional>
#include <string_view>
#include <vector>

namespace sharding {

using ShardId = uint32_t;

// Plain FNV-1a 64 (used for checksums).
uint64_t Fnv1a64(std::string_view data);

// Ring position hash: FNV-1a 64 followed by murmur3 fmix64 finalizer.
// Stable across machines/builds (unlike std::hash).
uint64_t HashKey(std::string_view key);

class HashRing {
 public:
  struct Point {
    uint64_t pos;
    ShardId shard;
  };

  explicit HashRing(uint32_t vnodes_per_shard);

  bool AddShard(ShardId id);     // false if already present
  bool RemoveShard(ShardId id);  // false if not present
  bool HasShard(ShardId id) const;

  // nullopt only when the ring has no shards.
  std::optional<ShardId> Lookup(std::string_view key) const;

  size_t NumShards() const { return shards_.size(); }
  uint32_t vnodes() const { return vnodes_; }
  const std::vector<Point>& points() const { return points_; }  // for tests

 private:
  void Rebuild();

  uint32_t vnodes_;
  std::vector<ShardId> shards_;  // sorted, unique
  std::vector<Point> points_;    // sorted by pos, unique pos
};

}  // namespace sharding