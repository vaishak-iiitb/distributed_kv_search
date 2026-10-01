# Skip List — Concepts & Full Implementation

**Phase 1, Step 1** of the distributed KV store build order. This is the
foundation the memtable sits on, so it needs to be correct and trusted
before anything else in Phase 1 leans on it.

One honest note before you use this: your own project plan's rule of
thumb is *"if you couldn't reproduce a piece of logic from memory a week
later and explain why it's correct, you shouldn't have let AI write it
for you."* Skip lists aren't Raft — they're a well-understood, bounded
data structure, not the part of this project that's actually hard or
where the interview-defensibility stakes are highest. But get real value
out of having the full code below: trace through an `Insert` and a
`Search` by hand on paper with a small example before you just build and
run it. That's what makes it yours.

---

## 1. The Core Idea

A **skip list** is a sorted linked list with extra "express lane" layers
stacked on top, so you can skip over large chunks of the list instead of
walking it node by node.

```
Level 3:  HEAD ----------------------------> 50 -----------------> NIL
Level 2:  HEAD ------------> 20 ------------> 50 -----------------> NIL
Level 1:  HEAD ------> 10 -> 20 ------> 40 -> 50 ------------------> NIL
Level 0:  HEAD -> 5 -> 10 -> 20 -> 30 -> 40 -> 50 -> 60 -> 70 -> 80 -> NIL
```

- **Level 0** is a normal sorted linked list containing *every* key.
- Each level above it contains a random subset of the level below,
  roughly half as many nodes (because each node is promoted to the next
  level up with probability `p`, usually `p = 0.5`).
- To search, you start at the top-left (`head_`), move **right** as far
  as you can without overshooting the target key, then **drop down** a
  level and repeat. This gives expected **O(log n)** search, insert, and
  delete — the same asymptotic complexity as a balanced tree, without
  any rotation logic.

### Why this project uses a skip list instead of a balanced tree

From your design doc: RocksDB, LevelDB, and Cassandra all use skip
lists for their memtables because skip lists support **concurrent
reads far more simply than a balanced tree does** — there's no
rebalancing operation that needs to lock or rotate a whole subtree.
You're not implementing concurrency yet, but this is *why* the
industry standard is a skip list here, not a red-black tree.

### The randomization trick

When you insert a new node, you don't choose its height deliberately —
you flip a coin (metaphorically: `rand() < p`) repeatedly. Start the
new node at level 0. Keep promoting it to the next level up as long as
the coin keeps landing the "promote" way, capped at some `max_level`.
With `p = 0.5`:

- ~50% of nodes only exist at level 0
- ~25% of nodes reach level 1
- ~12.5% reach level 2
- ...and so on, geometrically shrinking

This is *why* the level structure ends up looking like the diagram
above without you ever explicitly rebalancing anything. It's
probabilistic, not deterministic — that's the whole trade you're
making versus a balanced tree.

---

## 2. The "Update Array" Pattern

This is the one idea that unlocks all three operations (`Insert`,
`Search`, `Remove`), so it's worth understanding on its own before
looking at code.

To insert or delete a node at key `k`, you first need to find, **at
every level**, the rightmost node whose key is still less than `k`.
Call this the node "just before where `k` would go" at that level.
Collect one such node per level into an array called `update[]`
(`update[i]` = the node just before the insertion point, at level `i`).

Walking down from the top level to level 0, moving right while the
next key is still less than `k`, and recording where you stopped at
each level — that single traversal builds the entire `update[]` array
in one pass. Once you have it:

- **Insert**: splice the new node in after `update[i]` at every level
  up to the new node's random height.
- **Remove**: unsplice the target node from after `update[i]` at every
  level it appears in.
- **Search**: you don't need `update[]` at all — just keep walking
  right-then-down until `forward[0]` either matches `k` or you've
  proven `k` isn't present.

---

## 3. Complexity Recap

| Operation | Average case | Worst case (rare) |
|---|---|---|
| Search    | O(log n) | O(n) |
| Insert    | O(log n) | O(n) |
| Delete    | O(log n) | O(n) |
| Space     | O(n) expected (bounded by `1/(1-p)` pointers/node on average) | — |

The worst case only happens if you get extraordinarily unlucky with
the coin flips (e.g., every node happens to land at level 0). This is
why it's called a *probabilistic* data structure — correctness never
depends on the randomness, only performance does.

---

## 4. Build Setup

`CMakeLists.txt` (repo root):

```cmake
cmake_minimum_required(VERSION 3.16)
project(distributed_kv_store CXX)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -Wall -Wextra -g")

enable_testing()

add_library(storage
    src/storage/skiplist.cpp
)
target_include_directories(storage PUBLIC src)

add_executable(test_skiplist test/unit/storage/test_skiplist.cpp)
target_link_libraries(test_skiplist PRIVATE storage)
add_test(NAME test_skiplist COMMAND test_skiplist)
```

Build and run:

```bash
cmake -B build
cmake --build build
./build/test_skiplist
```

---

## 5. Full Code

### `src/storage/skiplist.h`

```cpp
#pragma once

#include <cstdlib>
#include <ctime>
#include <string>
#include <vector>

// A skip list mapping string keys to string values, used as the memtable's
// backing structure. Not thread-safe yet — concurrency comes later (Phase 2+
// territory, once this is trusted in isolation).
class SkipList {
 public:
  explicit SkipList(int max_level = 16, double p = 0.5);
  ~SkipList();

  // Non-copyable for now — the raw pointer ownership below doesn't have a
  // deep-copy story yet, and nothing in this project needs to copy one.
  SkipList(const SkipList&) = delete;
  SkipList& operator=(const SkipList&) = delete;

  // Insert or overwrite the value for `key`.
  void Insert(const std::string& key, const std::string& value);

  // Look up `key`. Returns true and sets `out_value` if found.
  bool Search(const std::string& key, std::string* out_value) const;

  // Remove `key`. Returns true if it was present.
  bool Remove(const std::string& key);

  // For testing/debugging: dump all keys in sorted order (level 0 walk).
  std::vector<std::string> SortedKeys() const;

  // For debugging: how many levels are actually in use right now.
  int CurrentLevel() const { return current_level_; }

 private:
  struct Node {
    std::string key;
    std::string value;
    std::vector<Node*> forward;  // forward[i] = next node at level i
    explicit Node(int level, std::string k = "", std::string v = "")
        : key(std::move(k)), value(std::move(v)), forward(level, nullptr) {}
  };

  // Coin-flip a height for a new node: starts at level 0, promoted to the
  // next level up with probability p_, capped at max_level_ - 1.
  int RandomLevel() const;

  int max_level_;
  double p_;
  int current_level_;  // highest level actually in use right now (>= 0)
  Node* head_;          // sentinel node, forward[] sized to max_level_
};
```

### `src/storage/skiplist.cpp`

```cpp
#include "storage/skiplist.h"

#include <cstdlib>

SkipList::SkipList(int max_level, double p)
    : max_level_(max_level), p_(p), current_level_(0) {
  std::srand(static_cast<unsigned>(std::time(nullptr)));
  head_ = new Node(max_level_);  // sentinel: real keys are never compared
                                  // against head_ itself, only its forward[]
}

SkipList::~SkipList() {
  Node* cur = head_;
  while (cur != nullptr) {
    Node* next = cur->forward[0];
    delete cur;
    cur = next;
  }
}

int SkipList::RandomLevel() const {
  int level = 0;
  // Keep promoting while the coin flip says "yes" and we haven't hit the cap.
  while ((static_cast<double>(std::rand()) / RAND_MAX) < p_ &&
         level < max_level_ - 1) {
    level++;
  }
  return level;
}

void SkipList::Insert(const std::string& key, const std::string& value) {
  std::vector<Node*> update(max_level_, head_);
  Node* cur = head_;

  // Walk down from the current top level, moving right while the next
  // node's key is still less than the target key. Record the last node
  // visited at each level — this is the "update array" from section 2.
  for (int i = current_level_; i >= 0; i--) {
    while (cur->forward[i] != nullptr && cur->forward[i]->key < key) {
      cur = cur->forward[i];
    }
    update[i] = cur;
  }

  Node* candidate = update[0]->forward[0];

  // Key already exists — overwrite in place, no restructuring needed.
  if (candidate != nullptr && candidate->key == key) {
    candidate->value = value;
    return;
  }

  // Key doesn't exist — pick a height and splice in a new node.
  int new_level = RandomLevel();

  // If the new node reaches higher than any existing node, extend the
  // update array's higher levels to point at head_ (nothing to jump past).
  if (new_level > current_level_) {
    for (int i = current_level_ + 1; i <= new_level; i++) {
      update[i] = head_;
    }
    current_level_ = new_level;
  }

  Node* new_node = new Node(new_level + 1, key, value);
  for (int i = 0; i <= new_level; i++) {
    new_node->forward[i] = update[i]->forward[i];
    update[i]->forward[i] = new_node;
  }
}

bool SkipList::Search(const std::string& key, std::string* out_value) const {
  Node* cur = head_;
  for (int i = current_level_; i >= 0; i--) {
    while (cur->forward[i] != nullptr && cur->forward[i]->key < key) {
      cur = cur->forward[i];
    }
  }
  cur = cur->forward[0];
  if (cur != nullptr && cur->key == key) {
    if (out_value != nullptr) {
      *out_value = cur->value;
    }
    return true;
  }
  return false;
}

bool SkipList::Remove(const std::string& key) {
  std::vector<Node*> update(max_level_, head_);
  Node* cur = head_;

  for (int i = current_level_; i >= 0; i--) {
    while (cur->forward[i] != nullptr && cur->forward[i]->key < key) {
      cur = cur->forward[i];
    }
    update[i] = cur;
  }

  Node* target = update[0]->forward[0];
  if (target == nullptr || target->key != key) {
    return false;  // not present
  }

  // Unsplice target from every level it appears in.
  for (int i = 0; i <= current_level_; i++) {
    if (update[i]->forward[i] != target) {
      break;  // target doesn't reach this level, and nothing higher does
    }
    update[i]->forward[i] = target->forward[i];
  }

  delete target;

  // Shrink current_level_ if the top levels are now empty.
  while (current_level_ > 0 && head_->forward[current_level_] == nullptr) {
    current_level_--;
  }

  return true;
}

std::vector<std::string> SkipList::SortedKeys() const {
  std::vector<std::string> keys;
  Node* cur = head_->forward[0];
  while (cur != nullptr) {
    keys.push_back(cur->key);
    cur = cur->forward[0];
  }
  return keys;
}
```

### `test/unit/storage/test_skiplist.cpp`

```cpp
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
```

---

## 6. Things Worth Understanding, Not Just Running

- **Why does `Insert`'s overwrite path skip `RandomLevel()` entirely?**
  Because the node's structural position in the list doesn't change —
  only its value does. Re-randomizing the height would be pointless
  work and would actually be *wrong*: the node's forward pointers at
  its existing levels are already correct.

- **Why does `Remove`'s unsplice loop `break` early?**
  Once `update[i]->forward[i] != target`, that means the target node
  never existed at level `i` (it wasn't tall enough), and by
  construction it can't exist at any level above `i` either — so
  there's nothing left to unsplice.

- **Why walk `current_level_` down to 0 in `Search`, not up?**
  Because higher levels have fewer nodes — starting there lets you
  cover more ground per step before dropping to a denser level. This
  is the entire mechanism that makes it faster than an O(n) linear
  scan.

- **What would break this under concurrent access?**
  Right now, two threads calling `Insert` at once could corrupt
  `update[]`'s pointers mid-splice. Real systems (RocksDB included)
  solve this with lock-free CAS-based splicing rather than a single
  mutex around the whole structure — one reason the design doc calls
  out skip lists as friendlier to concurrent access than a tree in the
  first place. Not needed yet; worth remembering for later.

---

## 7. Next Step (Phase 1, Step 2)

Per the build order: WAL writer + reader/replay, *then* wrap this
skip list in a `Memtable` class with a size-threshold flush trigger.
Don't start that until `test_skiplist` passes cleanly and you've
traced through at least one `Insert` by hand.
