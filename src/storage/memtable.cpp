#include "storage/memtable.h"

const std::string Memtable::kTombstoneMarker = "";

Memtable::Memtable(const std::string& wal_path, size_t size_threshold_bytes)
    : wal_(wal_path),
      size_threshold_bytes_(size_threshold_bytes),
      approximate_size_(0) {}

void Memtable::ApplyToSkipList(const std::string& key,
                                const std::string& value, OpType op_type) {
  std::string tagged_value;
  tagged_value.reserve(value.size() + 1);
  tagged_value.push_back(static_cast<char>(op_type));
  tagged_value += value;

  skiplist_.Insert(key, tagged_value);
  approximate_size_ += key.size() + value.size() + kPerRecordOverhead;
}

void Memtable::Decode(const std::string& tagged_value, OpType* op_type,
                       std::string* value) {
  *op_type = static_cast<OpType>(tagged_value[0]);
  *value = tagged_value.substr(1);
}

bool Memtable::Put(const std::string& key, const std::string& value) {
  if (!wal_.Append(key, value, OpType::kPut)) {
    return false;
  }
  ApplyToSkipList(key, value, OpType::kPut);
  return true;
}

bool Memtable::Delete(const std::string& key) {
  if (!wal_.Append(key, kTombstoneMarker, OpType::kDelete)) {
    return false;
  }
  ApplyToSkipList(key, kTombstoneMarker, OpType::kDelete);
  return true;
}

GetResult Memtable::Get(const std::string& key, std::string* out_value) const {
  std::string tagged_value;
  if (!skiplist_.Search(key, &tagged_value)) {
    return GetResult::kNotPresent;
  }

  OpType op_type;
  std::string value;
  Decode(tagged_value, &op_type, &value);

  if (op_type == OpType::kDelete) {
    return GetResult::kDeleted;
  }
  if (out_value != nullptr) {
    *out_value = value;
  }
  return GetResult::kFound;
}

bool Memtable::IsFull() const {
  return approximate_size_ >= size_threshold_bytes_;
}

size_t Memtable::RecoverFromWal() {
  return wal_.Replay([this](const WalRecord& record) {
    ApplyToSkipList(record.key, record.value, record.op_type);
  });
}

std::vector<MemtableEntry> Memtable::AllSorted() const {
  std::vector<MemtableEntry> result;
  for (const auto& key : skiplist_.SortedKeys()) {
    std::string tagged_value;
    skiplist_.Search(key, &tagged_value);  // guaranteed present — key just
                                            // came from SortedKeys()

    OpType op_type;
    std::string value;
    Decode(tagged_value, &op_type, &value);

    result.push_back(MemtableEntry{key, value, op_type});
  }
  return result;
}