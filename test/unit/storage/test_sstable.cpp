#include <cassert>
#include <cstdio>
#include <iostream>
#include <string>
#include <vector>
#include "storage/memtable.h"
#include "storage/sstable.h"

namespace {
const char* kSSTablePath = "test_sstable_scratch.sst";
const char* kWalPath = "test_sstable_wal_scratch.log";
}

int main() {
  std::remove(kSSTablePath);

  // --- Basic write + read round-trip, spanning multiple sparse index
  //     entries (kIndexInterval == 16, so use enough keys to exercise
  //     more than one index bucket and the linear-scan-forward path) ---
  {
    std::vector<MemtableEntry> entries;
    for (int i = 0; i < 50; i++) {
      char buf[16];
      std::snprintf(buf, sizeof(buf), "key%03d", i);  // zero-padded: sorts
                                                        // correctly as strings
      entries.push_back(
          MemtableEntry{buf, "value" + std::to_string(i), OpType::kPut});
    }

    assert(SSTable::WriteFromEntries(kSSTablePath, entries));

    SSTable sst(kSSTablePath);
    std::string v;

    // Found: first, middle, last — exercises different sparse index buckets
    assert(sst.Get("key000", &v) == SSTableGetResult::kFound &&
           v == "value0");
    assert(sst.Get("key025", &v) == SSTableGetResult::kFound &&
           v == "value25");
    assert(sst.Get("key049", &v) == SSTableGetResult::kFound &&
           v == "value49");

    // Not present: before range, between existing keys, after range
    assert(sst.Get("key000_before", &v) == SSTableGetResult::kNotPresent);
    assert(sst.Get("aaa_way_before_everything", &v) ==
           SSTableGetResult::kNotPresent);
    assert(sst.Get("zzz_way_after_everything", &v) ==
           SSTableGetResult::kNotPresent);
  }

  // --- Tombstones survive the round-trip correctly ---
  std::remove(kSSTablePath);
  {
    std::vector<MemtableEntry> entries = {
        MemtableEntry{"apple", "red", OpType::kPut},
        MemtableEntry{"banana", "yellow", OpType::kPut},
        MemtableEntry{"cherry", "", OpType::kDelete},
    };
    assert(SSTable::WriteFromEntries(kSSTablePath, entries));

    SSTable sst(kSSTablePath);
    std::string v;
    assert(sst.Get("apple", &v) == SSTableGetResult::kFound && v == "red");
    assert(sst.Get("cherry", &v) == SSTableGetResult::kDeleted);
    assert(sst.Get("durian", &v) == SSTableGetResult::kNotPresent);
  }

  // --- Empty SSTable (edge case: memtable flushed with zero entries) ---
  std::remove(kSSTablePath);
  {
    std::vector<MemtableEntry> empty;
    assert(SSTable::WriteFromEntries(kSSTablePath, empty));

    SSTable sst(kSSTablePath);
    std::string v;
    assert(sst.Get("anything", &v) == SSTableGetResult::kNotPresent);
  }

  // --- Integration: real Memtable -> AllSorted() -> SSTable, end to end ---
  std::remove(kSSTablePath);
  std::remove(kWalPath);
  {
    Memtable mt(kWalPath, /*size_threshold_bytes=*/1 << 20);
    assert(mt.Put("apple", "red"));
    assert(mt.Put("banana", "yellow"));
    assert(mt.Put("cherry", "dark red"));
    assert(mt.Delete("banana"));  // deleted after being written — tombstone
    assert(mt.Put("date", "brown"));

    auto entries = mt.AllSorted();
    // Sorted order check
    assert(entries[0].key == "apple");
    assert(entries[1].key == "banana");
    assert(entries[2].key == "cherry");
    assert(entries[3].key == "date");

    assert(SSTable::WriteFromEntries(kSSTablePath, entries));

    SSTable sst(kSSTablePath);
    std::string v;
    assert(sst.Get("apple", &v) == SSTableGetResult::kFound && v == "red");
    assert(sst.Get("banana", &v) == SSTableGetResult::kDeleted);
    assert(sst.Get("cherry", &v) == SSTableGetResult::kFound &&
           v == "dark red");
    assert(sst.Get("date", &v) == SSTableGetResult::kFound && v == "brown");
    assert(sst.Get("never_written", &v) == SSTableGetResult::kNotPresent);
  }

  std::cout << "All SSTable tests passed.\n";
  return 0;
}