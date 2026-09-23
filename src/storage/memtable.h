#pragma once

#include <cstddef>
#include <string>
#include <vector>

#include "storage/skiplist.h"
#include "storage/wal.h"

enum class GetResult {
  kFound,       // key present with a real value
  kDeleted,     // key present as a tombstone — definitely deleted
  kNotPresent,  // key not in this memtable at all; caller should keep
                // checking older memtables/SSTables
};

// One decoded entry, used when flushing a memtable to an SSTable.
struct MemtableEntry {
  std::string key;
  std::string value;  // empty/unused when op_type == kDelete
  OpType op_type;
};

// Wraps a skip list with write-ahead logging and a size-threshold flush
// trigger. This is the in-memory half of the LSM-tree write path from
// design doc §1.3, steps 1-3. Step 4 (actual flush to SSTable) happens
// one level up, using AllSorted() below.
class Memtable {
 public:
  Memtable(const std::string& wal_path, size_t size_threshold_bytes);

  Memtable(const Memtable&) = delete;
  Memtable& operator=(const Memtable&) = delete;

  bool Put(const std::string& key, const std::string& value);
  bool Delete(const std::string& key);
  GetResult Get(const std::string& key, std::string* out_value) const;
  bool IsFull() const;
  size_t ApproximateSize() const { return approximate_size_; }
  size_t RecoverFromWal();
  Wal& GetWal() { return wal_; }

  // Returns every entry in this memtable, in sorted key order, decoded
  // (op_type separated back out from the tagged skip-list value). Used
  // by SSTable::WriteFromEntries when flushing a frozen memtable.
  std::vector<MemtableEntry> AllSorted() const;

 private:
  static constexpr size_t kPerRecordOverhead = 32;

  void ApplyToSkipList(const std::string& key, const std::string& value,
                        OpType op_type);

  // Splits a tagged skip-list value (1-byte op_type prefix + payload)
  // back into its op_type and value. Shared by Get() and AllSorted() so
  // the decoding logic lives in exactly one place.
  static void Decode(const std::string& tagged_value, OpType* op_type,
                      std::string* value);

  SkipList skiplist_;
  Wal wal_;
  size_t size_threshold_bytes_;
  size_t approximate_size_;

  static const std::string kTombstoneMarker;
};