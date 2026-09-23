#pragma once

#include <cstdint>
#include <string>
#include <vector>

#include "storage/bloom_filter.h"
#include "storage/memtable.h"

enum class SSTableGetResult {
  kFound,
  kDeleted,
  kNotPresent,
};

class SSTable {
 public:
  static bool WriteFromEntries(const std::string& path,
                                const std::vector<MemtableEntry>& entries);

  explicit SSTable(const std::string& path);

  SSTableGetResult Get(const std::string& key, std::string* out_value) const;

  // Reads every record in this file, in sorted order, decoded back into
  // MemtableEntry form (key, value, op_type — including tombstones, which
  // are returned as-is, not filtered out here). Used by compaction to
  // merge multiple SSTables together.
  std::vector<MemtableEntry> AllEntries() const;

  const std::string& Path() const { return path_; }

 private:
  struct IndexEntry {
    std::string key;
    uint64_t offset;
  };

  static constexpr int kIndexInterval = 16;
  static constexpr uint32_t kMagicNumber = 0x53535442;  // ASCII "SSTB"

  std::string path_;
  std::vector<IndexEntry> sparse_index_;
  BloomFilter bloom_filter_;
  uint64_t data_section_end_;
};