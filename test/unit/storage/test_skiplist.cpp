#include <cassert>
#include <iostream>
#include "storage/skiplist.h"

int main() {
  SkipList sl;

  // Insert + search
  sl.Insert("banana", "yellow");
  sl.Insert("apple", "red");
  sl.Insert("cherry", "dark red");

  std::string v;
  assert(sl.Search("apple", &v) && v == "red");
  assert(sl.Search("banana", &v) && v == "yellow");
  assert(!sl.Search("durian", &v));  // not inserted

  // Overwrite
  sl.Insert("apple", "green");
  assert(sl.Search("apple", &v) && v == "green");

  // Sorted order
  auto keys = sl.SortedKeys();
  assert((keys == std::vector<std::string>{"apple", "banana", "cherry"}));

  // Remove
  assert(sl.Remove("banana"));
  assert(!sl.Search("banana", &v));
  assert(!sl.Remove("banana"));  // already gone, second remove is a no-op

  // Stress: several hundred random inserts, verify sorted order holds and
  // every inserted key is actually findable afterward.
  SkipList big;
  for (int i = 0; i < 500; i++) {
    std::string key = "key" + std::to_string(i);
    big.Insert(key, "v" + std::to_string(i));
  }
  auto big_keys = big.SortedKeys();
  assert(big_keys.size() == 500);
  for (size_t i = 1; i < big_keys.size(); i++) {
    assert(big_keys[i - 1] < big_keys[i]);
  }
  for (int i = 0; i < 500; i++) {
    std::string key = "key" + std::to_string(i);
    assert(big.Search(key, &v) && v == "v" + std::to_string(i));
  }

  // Sanity check the level distribution looks geometric, not degenerate.
  std::cout << "Top level in use after 500 inserts: " << big.CurrentLevel()
            << " (expect roughly log2(500) ~= 9, capped at 15)\n";

  std::cout << "All skip list tests passed.\n";
  return 0;
}