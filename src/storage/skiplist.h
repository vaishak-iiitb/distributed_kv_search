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