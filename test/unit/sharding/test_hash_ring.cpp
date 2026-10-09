#include <cmath>
#include <cstdio>
#include <cstdlib>
#include <map>
#include <string>

#include "sharding/hash_ring.h"

using namespace sharding;

static int g_failures = 0;
#define CHECK(cond)                                                            \
  do {                                                                         \
    if (!(cond)) {                                                             \
      std::fprintf(stderr, "FAIL %s:%d: %s\n", __FILE__, __LINE__, #cond);     \
      ++g_failures;                                                            \
    }                                                                          \
  } while (0)

static std::string Key(int i) { return "key-" + std::to_string(i); }

static HashRing MakeRing(int shards, uint32_t vnodes) {
  HashRing r(vnodes);
  for (int i = 0; i < shards; ++i) r.AddShard(i);
  return r;
}

static void TestGoldenHashes() {
  // Frozen values: if these change, routing changes for every existing deployment.
  CHECK(HashKey("") == 15503018906515740718ULL);
  CHECK(HashKey("a") == 4875499902769123557ULL);
  CHECK(HashKey("hello") == 16128656630293670621ULL);
  CHECK(HashKey("user:42") == 7435572394901849860ULL);
  HashRing r = MakeRing(3, 128);
  CHECK(*r.Lookup("alpha") == 2);
  CHECK(*r.Lookup("beta") == 2);
  CHECK(*r.Lookup("gamma") == 1);
  CHECK(*r.Lookup("user:42") == 0);
  CHECK(*r.Lookup("key-1") == 0);
}

static void TestEmptyAndBasics() {
  HashRing r(16);
  CHECK(!r.Lookup("x").has_value());
  CHECK(r.AddShard(7));
  CHECK(!r.AddShard(7));
  CHECK(r.HasShard(7));
  CHECK(*r.Lookup("anything") == 7);
  CHECK(r.RemoveShard(7));
  CHECK(!r.RemoveShard(7));
  CHECK(!r.Lookup("x").has_value());
}

static void TestDeterminismAndOrderIndependence() {
  HashRing a(64), b(64);
  for (int s : {0, 1, 2, 3}) a.AddShard(s);
  for (int s : {3, 1, 0, 2}) b.AddShard(s);
  for (int i = 0; i < 100000; ++i) CHECK(*a.Lookup(Key(i)) == *b.Lookup(Key(i)));
  // add-then-remove returns to the same routing
  HashRing c = MakeRing(3, 64), d = MakeRing(3, 64);
  d.AddShard(9);
  d.RemoveShard(9);
  for (int i = 0; i < 20000; ++i) CHECK(*c.Lookup(Key(i)) == *d.Lookup(Key(i)));
}

static void TestWraparound() {
  HashRing r(1);
  r.AddShard(0);
  r.AddShard(1);
  const auto& pts = r.points();
  CHECK(pts.size() == 2);
  uint64_t last = pts.back().pos;
  int found = 0;
  for (int i = 0; i < 1000000 && found < 5; ++i) {
    std::string k = Key(i);
    if (HashKey(k) > last) {
      CHECK(*r.Lookup(k) == pts.front().shard);  // must wrap to the first point
      ++found;
    }
  }
  CHECK(found > 0);
}

static void TestDistribution() {
  const int kShards = 3, kKeys = 100000;
  const uint32_t V = 150;
  HashRing r = MakeRing(kShards, V);
  std::map<ShardId, int> cnt;
  for (int i = 0; i < kKeys; ++i) cnt[*r.Lookup(Key(i))]++;
  double mean = double(kKeys) / kShards;
  double tol = 3.0 / std::sqrt(double(V));  // ~3 sigma of the 1/sqrt(V) estimate
  for (auto& [s, c] : cnt) {
    double dev = std::fabs(c - mean) / mean;
    std::printf("  shard %u: %d keys (%.1f%% off mean)\n", s, c, dev * 100);
    CHECK(dev < tol);
  }
  CHECK(cnt.size() == (size_t)kShards);
}

static void TestAddMinimalRemap() {
  const int kKeys = 100000;
  HashRing before = MakeRing(3, 128), after = MakeRing(3, 128);
  after.AddShard(3);
  int moved = 0, moved_elsewhere = 0;
  for (int i = 0; i < kKeys; ++i) {
    ShardId a = *before.Lookup(Key(i)), b = *after.Lookup(Key(i));
    if (a != b) {
      ++moved;
      if (b != 3) ++moved_elsewhere;
    }
  }
  double frac = double(moved) / kKeys;
  std::printf("  add 3->4 shards: %.1f%% of keys moved\n", frac * 100);
  CHECK(moved_elsewhere == 0);  // exact property, not statistical
  CHECK(frac > 0.18 && frac < 0.32);
}

static void TestRemoveOnlyMovesRemovedShardKeys() {
  const int kKeys = 100000;
  HashRing before = MakeRing(4, 128), after = MakeRing(4, 128);
  after.RemoveShard(2);
  int bad = 0;
  for (int i = 0; i < kKeys; ++i) {
    ShardId a = *before.Lookup(Key(i)), b = *after.Lookup(Key(i));
    if (a != 2 && a != b) ++bad;  // keys not on shard 2 must not move
    CHECK(b != 2);
  }
  CHECK(bad == 0);
}

static void ExperimentImbalanceVsVnodes() {
  std::printf("  vnodes -> max relative deviation from mean (4 shards, 100k keys)\n");
  for (uint32_t v : {1u, 8u, 32u, 128u, 256u, 1024u}) {
    HashRing r = MakeRing(4, v);
    std::map<ShardId, int> cnt;
    for (int i = 0; i < 100000; ++i) cnt[*r.Lookup(Key(i))]++;
    double worst = 0;
    for (auto& [s, c] : cnt) worst = std::fmax(worst, std::fabs(c - 25000.0) / 25000.0);
    std::printf("    V=%-5u %.1f%%\n", v, worst * 100);
  }
}

int main() {
  TestGoldenHashes();
  TestEmptyAndBasics();
  TestDeterminismAndOrderIndependence();
  TestWraparound();
  TestDistribution();
  TestAddMinimalRemap();
  TestRemoveOnlyMovesRemovedShardKeys();
  ExperimentImbalanceVsVnodes();
  if (g_failures) {
    std::fprintf(stderr, "%d check(s) failed\n", g_failures);
    return 1;
  }
  std::puts("test_hash_ring: all passed");
  return 0;
}