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