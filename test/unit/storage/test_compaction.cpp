#include <cassert>
#include <cstdio>
#include <iostream>
#include <vector>
#include "storage/compaction.h"
#include "storage/sstable.h"

namespace {
const char* kFile1 = "test_compaction_1.sst";
const char* kFile2 = "test_compaction_2.sst";
const char* kFile3 = "test_compaction_3.sst";
const char* kOutput = "test_compaction_output.sst";

void Cleanup() {
  std::remove(kFile1);
  std::remove(kFile2);
  std::remove(kFile3);
  std::remove(kOutput);
}
}  // namespace

int main() {
  Cleanup();

  // --- Disjoint keys: everything should just show up in the output ---
  {
    SSTable::WriteFromEntries(
        kFile1, {MemtableEntry{"apple", "red", OpType::kPut}});
    SSTable::WriteFromEntries(
        kFile2, {MemtableEntry{"banana", "yellow", OpType::kPut}});

    assert(Compactor::CompactAll({kFile1, kFile2}, kOutput));

    SSTable out(kOutput);
    std::string v;
    assert(out.Get("apple", &v) == SSTableGetResult::kFound && v == "red");
    assert(out.Get("banana", &v) == SSTableGetResult::kFound &&
           v == "yellow");
  }

  // --- Overlapping key: newest file's version must win ---
  Cleanup();
  {
    // kFile1 = oldest, kFile2 = newest, per CompactAll's oldest-first contract
    SSTable::WriteFromEntries(
        kFile1, {MemtableEntry{"apple", "red", OpType::kPut}});
    SSTable::WriteFromEntries(
        kFile2, {MemtableEntry{"apple", "green", OpType::kPut}});

    assert(Compactor::CompactAll({kFile1, kFile2}, kOutput));

    SSTable out(kOutput);
    std::string v;
    assert(out.Get("apple", &v) == SSTableGetResult::kFound && v == "green");
  }

  // --- Tombstone in the newest file: key must be gone entirely from the
  //     output, not written forward as a tombstone (section 3/4) ---
  Cleanup();
  {
    SSTable::WriteFromEntries(
        kFile1, {MemtableEntry{"apple", "red", OpType::kPut},
                 MemtableEntry{"banana", "yellow", OpType::kPut}});
    SSTable::WriteFromEntries(
        kFile2, {MemtableEntry{"banana", "", OpType::kDelete}});

    assert(Compactor::CompactAll({kFile1, kFile2}, kOutput));

    SSTable out(kOutput);
    std::string v;
    assert(out.Get("apple", &v) == SSTableGetResult::kFound && v == "red");
    // Not kDeleted — the tombstone itself should be GONE, not present.
    // Both are observably "not found" from Get()'s perspective here,
    // but AllEntries() below proves it's actually absent, not a
    // preserved tombstone record.
    assert(out.Get("banana", &v) == SSTableGetResult::kNotPresent);

    auto all = out.AllEntries();
    assert(all.size() == 1);  // only "apple" — banana's tombstone was
                               // dropped from the file entirely
  }

  // --- Three-way merge, mixed Put/Delete/Put across files ---
  Cleanup();
  {
    SSTable::WriteFromEntries(
        kFile1, {MemtableEntry{"apple", "red", OpType::kPut}});
    SSTable::WriteFromEntries(
        kFile2, {MemtableEntry{"apple", "green", OpType::kPut},
                 MemtableEntry{"banana", "yellow", OpType::kPut}});
    SSTable::WriteFromEntries(
        kFile3, {MemtableEntry{"banana", "", OpType::kDelete},
                 MemtableEntry{"cherry", "dark red", OpType::kPut}});

    assert(Compactor::CompactAll({kFile1, kFile2, kFile3}, kOutput));

    SSTable out(kOutput);
    std::string v;
    assert(out.Get("apple", &v) == SSTableGetResult::kFound && v == "green");
    assert(out.Get("banana", &v) == SSTableGetResult::kNotPresent);
    assert(out.Get("cherry", &v) == SSTableGetResult::kFound &&
           v == "dark red");

    // Output must be sorted.
    auto all = out.AllEntries();
    for (size_t i = 1; i < all.size(); i++) {
      assert(all[i - 1].key < all[i].key);
    }
  }

  // --- delete_inputs_on_success actually removes the input files ---
  Cleanup();
  {
    SSTable::WriteFromEntries(
        kFile1, {MemtableEntry{"apple", "red", OpType::kPut}});
    SSTable::WriteFromEntries(
        kFile2, {MemtableEntry{"banana", "yellow", OpType::kPut}});

    assert(Compactor::CompactAll({kFile1, kFile2}, kOutput,
                                  /*delete_inputs_on_success=*/true));

    std::ifstream f1_check(kFile1);
    std::ifstream f2_check(kFile2);
    assert(!f1_check.good());
    assert(!f2_check.good());

    // Output itself must still be intact and correct.
    SSTable out(kOutput);
    std::string v;
    assert(out.Get("apple", &v) == SSTableGetResult::kFound);
    assert(out.Get("banana", &v) == SSTableGetResult::kFound);
  }

  // --- ShouldCompact trigger ---
  {
    assert(!Compactor::ShouldCompact(/*current=*/4, /*threshold=*/4));
    assert(Compactor::ShouldCompact(/*current=*/5, /*threshold=*/4));
  }

  Cleanup();
  std::cout << "All compaction tests passed.\n";
  return 0;
}