#pragma once
// ShardMap: the cluster topology stored in the metadata group.
// Text encoding (line-safe, no whitespace):
//   SM1;v=<version>;vn=<vnodes>;s=<id>@<raft>/<client>,<raft>/<client>|<id>@...;ck=<16 hex>
// ck = FNV-1a 64 of everything before ";ck=", lowercase hex.
#include <cstdint>
#include <memory>
#include <string>
#include <string_view>
#include <vector>

#include "sharding/hash_ring.h"

namespace sharding {

struct NodeAddr {
  std::string raft_addr;    // host:port for Raft-to-Raft RPCs
  std::string client_addr;  // host:port for client-facing protocol
};

struct ShardInfo {
  ShardId id = 0;
  std::vector<NodeAddr> nodes;
};

enum class DecodeStatus { kOk, kCorrupt };

struct ShardMap {
  uint64_t version = 0;
  uint32_t vnodes = 128;
  std::vector<ShardInfo> shards;

  bool Validate(std::string* err = nullptr) const;
  const ShardInfo* Find(ShardId id) const;
  std::shared_ptr<const HashRing> BuildRing() const;

  std::string Encode() const;  // requires Validate() == true
  static DecodeStatus Decode(std::string_view text, ShardMap* out);
};

}  // namespace sharding