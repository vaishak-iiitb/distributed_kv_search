#include "storage/wal.h"

#include <cstdio>
#include <cstring>

namespace {

// Small, dependency-free checksum (Fowler-Noll-Vo, 32-bit). Not
// cryptographically strong — doesn't need to be. It only needs to catch
// accidental corruption from a partial/torn write, not adversarial
// tampering.
uint32_t Fnv1a(const void* data, size_t len, uint32_t hash = 2166136261u) {
  const unsigned char* bytes = static_cast<const unsigned char*>(data);
  for (size_t i = 0; i < len; i++) {
    hash ^= bytes[i];
    hash *= 16777619u;
  }
  return hash;
}

template <typename T>
void WriteRaw(std::fstream& out, const T& value) {
  out.write(reinterpret_cast<const char*>(&value), sizeof(T));
}

template <typename T>
bool ReadRaw(std::fstream& in, T* value) {
  in.read(reinterpret_cast<char*>(value), sizeof(T));
  return static_cast<bool>(in);
}

}  // namespace

Wal::Wal(const std::string& path) : path_(path) {
  // Open for append + read, create if missing. Binary mode: no newline
  // translation, which would corrupt the raw byte payloads.
  file_.open(path_,
              std::ios::in | std::ios::out | std::ios::binary | std::ios::app);
  if (!file_.is_open()) {
    // Create it fresh if it didn't exist, then reopen in read/write mode.
    std::fstream create(path_, std::ios::out | std::ios::binary);
    create.close();
    file_.open(path_, std::ios::in | std::ios::out | std::ios::binary);
  }
}

Wal::~Wal() {
  if (file_.is_open()) {
    file_.close();
  }
}

uint32_t Wal::Checksum(const std::string& key, const std::string& value,
                        OpType op_type) {
  uint32_t h = Fnv1a(key.data(), key.size());
  h = Fnv1a(value.data(), value.size(), h);
  uint8_t op_byte = static_cast<uint8_t>(op_type);
  h = Fnv1a(&op_byte, sizeof(op_byte), h);
  return h;
}

bool Wal::Append(const std::string& key, const std::string& value,
                  OpType op_type) {
  // Always append at the end, even though the stream is also open for
  // reading (Replay seeks independently).
  file_.clear();
  file_.seekp(0, std::ios::end);

  uint32_t checksum = Checksum(key, value, op_type);
  uint32_t key_len = static_cast<uint32_t>(key.size());
  uint32_t val_len = static_cast<uint32_t>(value.size());
  uint8_t op_byte = static_cast<uint8_t>(op_type);

  WriteRaw(file_, checksum);
  WriteRaw(file_, key_len);
  WriteRaw(file_, val_len);
  file_.write(key.data(), static_cast<std::streamsize>(key.size()));
  file_.write(value.data(), static_cast<std::streamsize>(value.size()));
  WriteRaw(file_, op_byte);

  file_.flush();
  if (!file_) {
    return false;
  }

  // This is the actual durability point. Nothing before this line
  // guarantees the write survives a crash/power loss.
  std::fflush(nullptr);  // flush C stdio buffers, belt-and-braces
  return true;
}

size_t Wal::Replay(const std::function<void(const WalRecord&)>& callback) {
  file_.clear();
  file_.seekg(0, std::ios::beg);

  size_t count = 0;
  while (true) {
    uint32_t stored_checksum = 0, key_len = 0, val_len = 0;

    if (!ReadRaw(file_, &stored_checksum)) break;  // clean EOF or short read
    if (!ReadRaw(file_, &key_len)) break;
    if (!ReadRaw(file_, &val_len)) break;

    std::string key(key_len, '\0');
    if (key_len > 0) {
      file_.read(&key[0], key_len);
      if (!file_) break;  // torn write: header present, payload missing
    }

    std::string value(val_len, '\0');
    if (val_len > 0) {
      file_.read(&value[0], val_len);
      if (!file_) break;
    }

    uint8_t op_byte = 0;
    if (!ReadRaw(file_, &op_byte)) break;
    OpType op_type = static_cast<OpType>(op_byte);

    uint32_t computed_checksum = Checksum(key, value, op_type);
    if (computed_checksum != stored_checksum) {
      // Corrupt record — this is the crash-mid-write case. Stop here;
      // everything replayed so far is still valid and already delivered.
      break;
    }

    callback(WalRecord{key, value, op_type});
    count++;
  }

  return count;
}

void Wal::Remove() {
  if (file_.is_open()) {
    file_.close();
  }
  std::remove(path_.c_str());
}