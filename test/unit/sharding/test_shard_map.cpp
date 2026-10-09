#include <cstdio>
#include <random>
#include <string>

#include "sharding/shard_map.h"

using namespace sharding;

static int g_failures = 0;
#define CHECK(cond)                                                            \
  do {                                                                         \
    if (!(cond)) {                                                             \
      std::fprintf(stderr, "FAIL %s:%d: %s\n", __FILE__, __LINE__, #cond);     \
      ++g_failures;                                                            \
    }                                                                          \
  } while (0)

static ShardMap Sample() {
  ShardMap m;
  m.version = 3;
  m.vnodes = 128;
  for (uint32_t s = 0; s < 3; ++s) {
    ShardInfo si;
    si.id = s;
    for (int n = 0; n < 3; ++n)
      si.nodes.push_back({"127.0.0.1:" + std::to_string(7200 + 10 * s + n),
                          "127.0.0.1:" + std::to_string(7300 + 10 * s + n)});
    m.shards.push_back(si);
  }
  return m;
}

static void TestRoundTrip() {
  ShardMap m = Sample();
  CHECK(m.Validate());
  std::string enc = m.Encode();
  CHECK(enc.find('\n') == std::string::npos && enc.find(' ') == std::string::npos);
  ShardMap out;
  CHECK(ShardMap::Decode(enc, &out) == DecodeStatus::kOk);
  CHECK(out.version == 3 && out.vnodes == 128 && out.shards.size() == 3);
  CHECK(out.shards[1].nodes[2].client_addr == "127.0.0.1:7312");
  CHECK(out.Encode() == enc);
}

static void TestRingEquivalence() {
  ShardMap m = Sample(), out;
  CHECK(ShardMap::Decode(m.Encode(), &out) == DecodeStatus::kOk);
  auto r1 = m.BuildRing(), r2 = out.BuildRing();
  for (int i = 0; i < 100000; ++i) {
    std::string k = "key-" + std::to_string(i);
    CHECK(*r1->Lookup(k) == *r2->Lookup(k));
  }
}

static void TestValidate() {
  std::string err;
  ShardMap m = Sample();
  m.version = 0;
  CHECK(!m.Validate(&err));
  m = Sample(); m.shards.clear();
  CHECK(!m.Validate());
  m = Sample(); m.shards[1].id = 0;  // duplicate id
  CHECK(!m.Validate());
  m = Sample(); m.shards[0].nodes.clear();
  CHECK(!m.Validate());
  m = Sample(); m.shards[0].nodes[0].raft_addr = "bad addr";
  CHECK(!m.Validate());
  m = Sample(); m.shards[0].nodes[0].client_addr = "a;b";
  CHECK(!m.Validate());
  m = Sample(); m.vnodes = 0;
  CHECK(!m.Validate());
}

static void TestEveryTruncationRejected() {
  std::string enc = Sample().Encode();
  for (size_t n = 0; n < enc.size(); ++n) {
    ShardMap out;
    CHECK(ShardMap::Decode(enc.substr(0, n), &out) == DecodeStatus::kCorrupt);
  }
}

static void TestEverySingleByteFlipRejected() {
  std::string enc = Sample().Encode();
  for (size_t i = 0; i < enc.size(); ++i) {
    std::string bad = enc;
    bad[i] = (bad[i] == 'x') ? 'y' : 'x';
    ShardMap out;
    CHECK(ShardMap::Decode(bad, &out) == DecodeStatus::kCorrupt);
  }
}

static void TestTrailingGarbageAndNoncanonical() {
  std::string enc = Sample().Encode();
  ShardMap out;
  CHECK(ShardMap::Decode(enc + "x", &out) == DecodeStatus::kCorrupt);
  CHECK(ShardMap::Decode(enc + "\n", &out) == DecodeStatus::kCorrupt);
  CHECK(ShardMap::Decode("", &out) == DecodeStatus::kCorrupt);
}

static void TestFuzz() {
  std::mt19937_64 rng(12345);
  std::string valid = Sample().Encode();
  for (int it = 0; it < 20000; ++it) {
    std::string s;
    if (it % 2 == 0) {
      size_t len = rng() % 200;
      for (size_t i = 0; i < len; ++i) s += char(rng() % 256);
    } else {
      s = valid;
      int flips = 1 + rng() % 4;
      for (int f = 0; f < flips; ++f) s[rng() % s.size()] = char(rng() % 256);
      if (s == valid) continue;
    }
    ShardMap out;
    CHECK(ShardMap::Decode(s, &out) == DecodeStatus::kCorrupt);
  }
}

int main() {
  TestRoundTrip();
  TestRingEquivalence();
  TestValidate();
  TestEveryTruncationRejected();
  TestEverySingleByteFlipRejected();
  TestTrailingGarbageAndNoncanonical();
  TestFuzz();
  if (g_failures) {
    std::fprintf(stderr, "%d check(s) failed\n", g_failures);
    return 1;
  }
  std::puts("test_shard_map: all passed");
  return 0;
}