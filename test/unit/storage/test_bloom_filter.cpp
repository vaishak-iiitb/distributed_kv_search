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