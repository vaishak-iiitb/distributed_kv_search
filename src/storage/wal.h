#pragma once

#include <cstdint>
#include <fstream>
#include <functional>
#include <string>

enum class OpType : uint8_t {
  kPut = 0,
  kDelete = 1,
};

struct WalRecord {
  std::string key;
  std::string value;  // empty/unused for kDelete
  OpType op_type;
};

// Append-only, fsync'd log of writes. Every Put/Delete is durably recorded
// here before being acknowledged to the client — this is the storage
// engine's entire crash-safety guarantee.
class Wal {
 public:
  // Opens (or creates) the WAL file at `path` for appending.
  explicit Wal(const std::string& path);
  ~Wal();

  Wal(const Wal&) = delete;
  Wal& operator=(const Wal&) = delete;

  // Appends one record and fsyncs before returning. Returns false on any
  // I/O failure (caller must NOT acknowledge the client if this fails).
  bool Append(const std::string& key, const std::string& value,
              OpType op_type);

  // Replays every valid record from the start of the file, in order,
  // invoking `callback` for each. Stops at the first corrupt or
  // incomplete record (crash-mid-write case) without erroring out —
  // that's expected recovery behavior, not a failure.
  // Returns the number of valid records replayed.
  size_t Replay(const std::function<void(const WalRecord&)>& callback);

  // Deletes the underlying WAL file. Call this only after a memtable
  // flush derived from this WAL is durably on disk as an SSTable.
  void Remove();

 private:
  static uint32_t Checksum(const std::string& key, const std::string& value,
                            OpType op_type);

  std::string path_;
  std::fstream file_;
};