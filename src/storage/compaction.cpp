#include "storage/compaction.h"

#include <algorithm>
#include <cstdio>
#include <unordered_map>

#include "storage/sstable.h"

bool Compactor::ShouldCompact(size_t current_file_count, size_t threshold) {
  return current_file_count > threshold;
}

bool Compactor::CompactAll(const std::vector<std::string>& input_paths,
                            const std::string& output_path,
                            bool delete_inputs_on_success) {
  // Resolve every key to its newest version by reading input files in
  // oldest-to-newest order and letting later files overwrite earlier
  // ones in the map — last write wins, matching "newest version wins"
  // from section 3.
  std::unordered_map<std::string, MemtableEntry> latest;

  for (const auto& path : input_paths) {
    SSTable sst(path);
    for (auto& entry : sst.AllEntries()) {
      latest[entry.key] = std::move(entry);
    }
  }

  // Build the merged, sorted output — dropping tombstones per section 4:
  // this merge covers every known SSTable, so a key whose newest version
  // is a delete has no older file left that could resurrect it.
  std::vector<MemtableEntry> merged;
  merged.reserve(latest.size());
  for (auto& [key, entry] : latest) {
    if (entry.op_type == OpType::kDelete) {
      continue;
    }
    merged.push_back(std::move(entry));
  }

  std::sort(merged.begin(), merged.end(),
            [](const MemtableEntry& a, const MemtableEntry& b) {
              return a.key < b.key;
            });

  if (!SSTable::WriteFromEntries(output_path, merged)) {
    return false;
  }

  if (delete_inputs_on_success) {
    for (const auto& path : input_paths) {
      std::remove(path.c_str());
    }
  }

  return true;
}