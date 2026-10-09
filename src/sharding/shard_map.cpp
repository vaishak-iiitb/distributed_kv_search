#include "sharding/shard_map.h"

#include <algorithm>
#include <charconv>
#include <cstdio>
#include <set>

namespace sharding {
namespace {

bool BadAddrChar(char c) {
  return c == ';' || c == '|' || c == ',' || c == '/' || c == '@' || c == '=' ||
         static_cast<unsigned char>(c) <= ' ' || static_cast<unsigned char>(c) >= 127;
}

bool AddrOk(const std::string& a) {
  return !a.empty() && std::none_of(a.begin(), a.end(), BadAddrChar);
}

std::vector<std::string_view> Split(std::string_view s, char sep) {
  std::vector<std::string_view> out;
  size_t start = 0;
  for (size_t i = 0; i <= s.size(); ++i) {
    if (i == s.size() || s[i] == sep) {
      out.push_back(s.substr(start, i - start));
      start = i + 1;
    }
  }
  return out;
}

template <typename T>
bool ParseUint(std::string_view s, T* out) {
  if (s.empty()) return false;
  T v{};
  auto [p, ec] = std::from_chars(s.data(), s.data() + s.size(), v);
  if (ec != std::errc() || p != s.data() + s.size()) return false;
  *out = v;
  return true;
}

bool StartsWith(std::string_view s, std::string_view prefix) {
  return s.substr(0, prefix.size()) == prefix;
}

std::string Hex16(uint64_t v) {
  char buf[17];
  std::snprintf(buf, sizeof buf, "%016llx", static_cast<unsigned long long>(v));
  return buf;
}

}  // namespace

bool ShardMap::Validate(std::string* err) const {
  auto fail = [&](const char* m) {
    if (err) *err = m;
    return false;
  };
  if (version == 0) return fail("version must be > 0");
  if (vnodes == 0 || vnodes > 10000) return fail("vnodes must be in [1,10000]");
  if (shards.empty()) return fail("no shards");
  std::set<ShardId> seen;
  for (const auto& s : shards) {
    if (!seen.insert(s.id).second) return fail("duplicate shard id");
    if (s.nodes.empty()) return fail("shard with no nodes");
    for (const auto& n : s.nodes) {
      if (!AddrOk(n.raft_addr) || !AddrOk(n.client_addr)) return fail("bad address");
    }
  }
  return true;
}

const ShardInfo* ShardMap::Find(ShardId id) const {
  for (const auto& s : shards)
    if (s.id == id) return &s;
  return nullptr;
}

std::shared_ptr<const HashRing> ShardMap::BuildRing() const {
  auto ring = std::make_shared<HashRing>(vnodes);
  for (const auto& s : shards) ring->AddShard(s.id);
  return ring;
}

std::string ShardMap::Encode() const {
  std::string body = "SM1;v=" + std::to_string(version) + ";vn=" + std::to_string(vnodes) + ";s=";
  for (size_t i = 0; i < shards.size(); ++i) {
    if (i) body += '|';
    body += std::to_string(shards[i].id) + '@';
    for (size_t j = 0; j < shards[i].nodes.size(); ++j) {
      if (j) body += ',';
      body += shards[i].nodes[j].raft_addr + '/' + shards[i].nodes[j].client_addr;
    }
  }
  return body + ";ck=" + Hex16(Fnv1a64(body));
}

DecodeStatus ShardMap::Decode(std::string_view text, ShardMap* out) {
  auto fields = Split(text, ';');
  if (fields.size() != 5 || fields[0] != "SM1") return DecodeStatus::kCorrupt;
  if (!StartsWith(fields[1], "v=") || !StartsWith(fields[2], "vn=") ||
      !StartsWith(fields[3], "s=") || !StartsWith(fields[4], "ck="))
    return DecodeStatus::kCorrupt;

  ShardMap m;
  if (!ParseUint(fields[1].substr(2), &m.version)) return DecodeStatus::kCorrupt;
  if (!ParseUint(fields[2].substr(3), &m.vnodes)) return DecodeStatus::kCorrupt;

  for (auto shard_txt : Split(fields[3].substr(2), '|')) {
    auto parts = Split(shard_txt, '@');
    if (parts.size() != 2) return DecodeStatus::kCorrupt;
    ShardInfo si;
    if (!ParseUint(parts[0], &si.id)) return DecodeStatus::kCorrupt;
    for (auto node_txt : Split(parts[1], ',')) {
      auto addrs = Split(node_txt, '/');
      if (addrs.size() != 2) return DecodeStatus::kCorrupt;
      si.nodes.push_back({std::string(addrs[0]), std::string(addrs[1])});
    }
    m.shards.push_back(std::move(si));
  }

  if (!m.Validate()) return DecodeStatus::kCorrupt;
  // Canonical-form check: rejects leading zeros, uppercase hex, bad checksum,
  // trailing garbage - anything that isn't exactly what Encode() would emit.
  if (m.Encode() != text) return DecodeStatus::kCorrupt;

  *out = std::move(m);
  return DecodeStatus::kOk;
}

}  // namespace sharding