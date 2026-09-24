#include "storage/kv_engine.h"

#include <algorithm>
#include <filesystem>

#include "storage/compaction.h"
#include "storage/sstable.h"

namespace fs = std::filesystem;

namespace {

bool HasSuffix(const std::string& s, const std::string& suffix) {
  return s.size() >= suffix.size() &&
         s.compare(s.size() - suffix.size(), suffix.size(), suffix) == 0;
}

// Parses the numeric id out of a filename like "wal_5.log" or "sst_12.sst"
// given its known prefix/suffix. Returns -1 if the filename doesn't
// actually match the expected pattern (defensive against stray files).
int ParseId(const std::string& filename, const std::string& prefix,
            const std::string& suffix) {
  if (filename.size() <= prefix.size() + suffix.size()) return -1;
  if (filename.compare(0, prefix.size(), prefix) != 0) return -1;
  if (!HasSuffix(filename, suffix)) return -1;

  std::string id_str = filename.substr(
      prefix.size(), filename.size() - prefix.size() - suffix.size());
  try {
    return std::stoi(id_str);
  } catch (...) {
    return -1;
  }
}

}  // namespace

KVEngine::KVEngine(const std::string& data_dir,
                    size_t memtable_threshold_bytes,
                    size_t compaction_threshold_files)
    : data_dir_(data_dir),
      memtable_threshold_bytes_(memtable_threshold_bytes),
      compaction_threshold_files_(compaction_threshold_files),
      next_sstable_id_(0),
      next_wal_id_(0) {
  fs::create_directories(data_dir_);

  std::string existing_wal_path;
  int existing_wal_id = -1;
  std::vector<std::pair<int, std::string>> existing_sstables;

  for (const auto& dir_entry : fs::directory_iterator(data_dir_)) {
    std::string name = dir_entry.path().filename().string();

    int wal_id = ParseId(name, "wal_", ".log");
    if (wal_id >= 0) {
      // Defensive: if more than one WAL file is somehow present, prefer
      // the most recent id. Normal operation only ever has at most one.
      if (wal_id > existing_wal_id) {
        existing_wal_id = wal_id;
        existing_wal_path = dir_entry.path().string();
      }
      continue;
    }

    int sst_id = ParseId(name, "sst_", ".sst");
    if (sst_id >= 0) {
      existing_sstables.emplace_back(sst_id, dir_entry.path().string());
    }
  }

  // Sort by id ascending == oldest-to-newest, matching what both the
  // read path (section 2) and Compactor::CompactAll require.
  std::sort(existing_sstables.begin(), existing_sstables.end());
  for (auto& [id, path] : existing_sstables) {
    sstable_paths_.push_back(path);
    next_sstable_id_ = std::max(next_sstable_id_, id + 1);
  }

  if (existing_wal_id >= 0) {
    // Recovery path: rebuild the memtable exactly as it was pre-crash.
    memtable_ = std::make_unique<Memtable>(existing_wal_path,
                                            memtable_threshold_bytes_);
    memtable_->RecoverFromWal();
    next_wal_id_ = existing_wal_id + 1;
  } else {
    std::string wal_path =
        data_dir_ + "/wal_" + std::to_string(next_wal_id_++) + ".log";
    memtable_ = std::make_unique<Memtable>(wal_path, memtable_threshold_bytes_);
  }
}

bool KVEngine::Put(const std::string& key, const std::string& value) {
  std::lock_guard<std::mutex> lock(mutex_);
  if (!memtable_->Put(key, value)) {
    return false;
  }
  MaybeFlush();
  return true;
}

bool KVEngine::Delete(const std::string& key) {
  std::lock_guard<std::mutex> lock(mutex_);
  if (!memtable_->Delete(key)) {
    return false;
  }
  MaybeFlush();
  return true;
}

bool KVEngine::Get(const std::string& key, std::string* out_value) {
  std::lock_guard<std::mutex> lock(mutex_);

  GetResult mt_result = memtable_->Get(key, out_value);
  if (mt_result == GetResult::kFound) return true;
  if (mt_result == GetResult::kDeleted) return false;

  // kNotPresent: fall through to SSTables, newest first (section 2).
  for (auto it = sstable_paths_.rbegin(); it != sstable_paths_.rend(); ++it) {
    SSTable sst(*it);
    SSTableGetResult sst_result = sst.Get(key, out_value);
    if (sst_result == SSTableGetResult::kFound) return true;
    if (sst_result == SSTableGetResult::kDeleted) return false;
    // kNotPresent: keep checking older files.
  }

  return false;
}

void KVEngine::MaybeFlush() {
  if (!memtable_->IsFull()) {
    return;
  }

  std::string sst_path =
      data_dir_ + "/sst_" + std::to_string(next_sstable_id_++) + ".sst";
  std::vector<MemtableEntry> entries = memtable_->AllSorted();

  if (!SSTable::WriteFromEntries(sst_path, entries)) {
    // Flush failed — roll back the id and keep writing to the current
    // memtable/WAL rather than losing data or corrupting the id sequence.
    next_sstable_id_--;
    return;
  }

  // Safe only now: entries are durably in sst_path, so the WAL that
  // produced them is no longer needed for recovery.
  memtable_->GetWal().Remove();
  sstable_paths_.push_back(sst_path);

  std::string wal_path =
      data_dir_ + "/wal_" + std::to_string(next_wal_id_++) + ".log";
  memtable_ = std::make_unique<Memtable>(wal_path, memtable_threshold_bytes_);

  MaybeCompact();
}

void KVEngine::MaybeCompact() {
  if (!Compactor::ShouldCompact(sstable_paths_.size(),
                                 compaction_threshold_files_)) {
    return;
  }

  std::string compacted_path =
      data_dir_ + "/sst_" + std::to_string(next_sstable_id_++) + ".sst";

  if (Compactor::CompactAll(sstable_paths_, compacted_path,
                             /*delete_inputs_on_success=*/true)) {
    sstable_paths_.clear();
    sstable_paths_.push_back(compacted_path);
  } else {
    next_sstable_id_--;
  }
}