#include <cassert>
#include <cstdio>
#include <iostream>
#include "storage/memtable.h"

namespace {
const char* kTestPath = "test_memtable_scratch.log";
}

int main() {
  std::remove(kTestPath);

  // --- Put + Get ---
  {
    Memtable mt(kTestPath, /*size_threshold_bytes=*/1024);

    assert(mt.Put("apple", "red"));
    assert(mt.Put("banana", "yellow"));

    std::string v;
    assert(mt.Get("apple", &v) == GetResult::kFound && v == "red");
    assert(mt.Get("banana", &v) == GetResult::kFound && v == "yellow");
    assert(mt.Get("durian", &v) == GetResult::kNotPresent);
  }

  // --- Overwrite ---
  std::remove(kTestPath);
  {
    Memtable mt(kTestPath, 1024);
    assert(mt.Put("apple", "red"));
    assert(mt.Put("apple", "green"));

    std::string v;
    assert(mt.Get("apple", &v) == GetResult::kFound && v == "green");
  }

  // --- Tombstones: Delete must NOT make the key look kNotPresent ---
  std::remove(kTestPath);
  {
    Memtable mt(kTestPath, 1024);
    assert(mt.Put("apple", "red"));
    assert(mt.Delete("apple"));

    std::string v;
    // Crucial distinction: kDeleted, not kNotPresent. A caller checking
    // older SSTables needs to know "stop, this is deleted" versus
    // "keep looking, this memtable just doesn't have it."
    assert(mt.Get("apple", &v) == GetResult::kDeleted);

    // A key that was truly never written is still kNotPresent.
    assert(mt.Get("never_written", &v) == GetResult::kNotPresent);
  }

  // --- Delete-then-Put resurrects the key ---
  std::remove(kTestPath);
  {
    Memtable mt(kTestPath, 1024);
    assert(mt.Put("apple", "red"));
    assert(mt.Delete("apple"));
    assert(mt.Put("apple", "blue"));

    std::string v;
    assert(mt.Get("apple", &v) == GetResult::kFound && v == "blue");
  }

  // --- IsFull() triggers once threshold is crossed ---
  std::remove(kTestPath);
  {
    Memtable mt(kTestPath, /*size_threshold_bytes=*/100);
    assert(!mt.IsFull());

    int i = 0;
    while (!mt.IsFull()) {
      mt.Put("key" + std::to_string(i), "some_value_here");
      i++;
      assert(i < 1000);  // safety net against an infinite loop if broken
    }
    assert(mt.IsFull());
    assert(mt.ApproximateSize() >= 100);
  }

  // --- RecoverFromWal reconstructs state after a simulated crash ---
  std::remove(kTestPath);
  {
    Memtable mt(kTestPath, 1024);
    assert(mt.Put("apple", "red"));
    assert(mt.Put("banana", "yellow"));
    assert(mt.Delete("apple"));
    // mt goes out of scope here — simulates process exit/crash. The
    // skip list's in-memory state is gone; only the WAL on disk remains.
  }
  {
    Memtable recovered(kTestPath, 1024);
    size_t n = recovered.RecoverFromWal();
    assert(n == 3);

    std::string v;
    assert(recovered.Get("apple", &v) == GetResult::kDeleted);
    assert(recovered.Get("banana", &v) == GetResult::kFound && v == "yellow");
  }

  std::cout << "All memtable tests passed.\n";
  return 0;
}