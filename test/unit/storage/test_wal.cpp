#include <cassert>
#include <cstdio>
#include <fstream>
#include <iostream>
#include <vector>
#include "storage/wal.h"

namespace {
const char* kTestPath = "test_wal_scratch.log";
}

int main() {
  std::remove(kTestPath);  // clean slate

  // --- Append + Replay round-trip ---
  {
    Wal wal(kTestPath);
    assert(wal.Append("apple", "red", OpType::kPut));
    assert(wal.Append("banana", "yellow", OpType::kPut));
    assert(wal.Append("apple", "", OpType::kDelete));
  }  // Wal destructor closes the file

  {
    Wal wal(kTestPath);
    std::vector<WalRecord> records;
    size_t n = wal.Replay([&](const WalRecord& r) { records.push_back(r); });

    assert(n == 3);
    assert(records.size() == 3);
    assert(records[0].key == "apple" && records[0].value == "red" &&
           records[0].op_type == OpType::kPut);
    assert(records[1].key == "banana" && records[1].value == "yellow" &&
           records[1].op_type == OpType::kPut);
    assert(records[2].key == "apple" && records[2].op_type == OpType::kDelete);
  }

  // --- Corruption / torn-write simulation ---
  // Write two good records, then hand-corrupt a byte inside the second
  // record's payload to simulate a crash mid-write, and verify Replay
  // stops cleanly at the corruption instead of returning garbage.
  {
    std::remove(kTestPath);
    Wal wal(kTestPath);
    assert(wal.Append("k1", "v1", OpType::kPut));
    assert(wal.Append("k2", "v2", OpType::kPut));
  }
  {
    // Flip a byte inside the file to break the second record's checksum.
    std::fstream f(kTestPath, std::ios::in | std::ios::out | std::ios::binary);
    f.seekp(20, std::ios::beg);  // lands inside the 2nd record's payload
    char corrupt = 'X';
    f.write(&corrupt, 1);
    f.close();
  }
  {
    Wal wal(kTestPath);
    std::vector<WalRecord> records;
    size_t n = wal.Replay([&](const WalRecord& r) { records.push_back(r); });

    // The first record must still survive; the corrupted second one must
    // not be replayed as if it were valid data.
    assert(n == 1);
    assert(records[0].key == "k1" && records[0].value == "v1");
  }

  // --- Remove() ---
  {
    Wal wal(kTestPath);
    wal.Remove();
    std::ifstream check(kTestPath);
    assert(!check.good());  // file should no longer exist
  }

  std::cout << "All WAL tests passed.\n";
  return 0;
}