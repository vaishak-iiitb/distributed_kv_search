#pragma once

#include <memory>
#include <mutex>
#include <string>
#include <vector>

#include "storage/memtable.h"

// Composes Memtable + a list of immutable SSTables + Compactor into one
// coherent, crash-recoverable key-value engine. See sections 1-3 of the
// accompanying doc for the read-order and recovery invariants this class
// is responsible for maintaining.
class KVEngine {
 public:
  // `data_dir`: where WAL and SSTable files live. Created if it doesn't
  // exist; scanned for pre-existing files (crash recovery) if it does.
  explicit KVEngine(const std::string& data_dir,
                     size_t memtable_threshold_bytes = 4 * 1024 * 1024,
                     size_t compaction_threshold_files = 4);

  KVEngine(const KVEngine&) = delete;
  KVEngine& operator=(const KVEngine&) = delete;

  // Returns false only on an underlying WAL write failure — caller must
  // not acknowledge the client in that case.
  bool Put(const std::string& key, const std::string& value);
  bool Delete(const std::string& key);

  // Returns true and sets *out_value if found; false if not present or
  // deleted (indistinguishable to the caller — a tombstone means "not
  // found" from here on).
  bool Get(const std::string& key, std::string* out_value);

 private:
  void MaybeFlush();    // caller must hold mutex_
  void MaybeCompact();  // caller must hold mutex_

  std::string data_dir_;
  size_t memtable_threshold_bytes_;
  size_t compaction_threshold_files_;

  std::mutex mutex_;
  std::unique_ptr<Memtable> memtable_;
  std::vector<std::string> sstable_paths_;  // oldest first, newest at back

  int next_sstable_id_;
  int next_wal_id_;
};