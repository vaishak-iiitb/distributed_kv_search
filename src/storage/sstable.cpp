#include "storage/sstable.h"

#include <algorithm>
#include <fstream>

namespace {

template <typename T>
void WriteRaw(std::ofstream& out, const T& value) {
  out.write(reinterpret_cast<const char*>(&value), sizeof(T));
}

template <typename T>
bool ReadRaw(std::ifstream& in, T* value) {
  in.read(reinterpret_cast<char*>(value), sizeof(T));
  return static_cast<bool>(in);
}

void WriteLengthPrefixedString(std::ofstream& out, const std::string& s) {
  uint32_t len = static_cast<uint32_t>(s.size());
  WriteRaw(out, len);
  out.write(s.data(), static_cast<std::streamsize>(s.size()));
}

bool ReadLengthPrefixedString(std::ifstream& in, std::string* s) {
  uint32_t len = 0;
  if (!ReadRaw(in, &len)) return false;
  s->assign(len, '\0');
  if (len > 0) {
    in.read(&(*s)[0], len);
    if (!in) return false;
  }
  return true;
}

}  // namespace

bool SSTable::WriteFromEntries(const std::string& path,
                                const std::vector<MemtableEntry>& entries) {
  std::ofstream out(path, std::ios::binary | std::ios::trunc);
  if (!out.is_open()) return false;

  std::vector<IndexEntry> index;
  BloomFilter bloom(entries.empty() ? 1 : entries.size());

  uint64_t offset = 0;
  for (size_t i = 0; i < entries.size(); i++) {
    const MemtableEntry& entry = entries[i];

    if (i % kIndexInterval == 0) {
      index.push_back(IndexEntry{entry.key, offset});
    }
    bloom.Add(entry.key);

    uint32_t key_len = static_cast<uint32_t>(entry.key.size());
    uint32_t val_len = static_cast<uint32_t>(entry.value.size());
    uint8_t op_byte = static_cast<uint8_t>(entry.op_type);

    WriteRaw(out, key_len);
    WriteRaw(out, val_len);
    WriteRaw(out, op_byte);
    out.write(entry.key.data(), static_cast<std::streamsize>(entry.key.size()));
    out.write(entry.value.data(),
              static_cast<std::streamsize>(entry.value.size()));

    offset += sizeof(key_len) + sizeof(val_len) + sizeof(op_byte) +
              entry.key.size() + entry.value.size();
  }

  uint64_t index_offset = offset;

  uint32_t index_count = static_cast<uint32_t>(index.size());
  WriteRaw(out, index_count);
  for (const auto& entry : index) {
    WriteLengthPrefixedString(out, entry.key);
    WriteRaw(out, entry.offset);
  }

  uint64_t bloom_offset = static_cast<uint64_t>(out.tellp());
  uint64_t num_bits = static_cast<uint64_t>(bloom.NumBits());
  int32_t num_hashes = static_cast<int32_t>(bloom.NumHashes());
  const std::vector<uint8_t>& bits = bloom.Bits();
  uint32_t byte_len = static_cast<uint32_t>(bits.size());

  WriteRaw(out, num_bits);
  WriteRaw(out, num_hashes);
  WriteRaw(out, byte_len);
  out.write(reinterpret_cast<const char*>(bits.data()),
            static_cast<std::streamsize>(bits.size()));

  WriteRaw(out, index_offset);
  WriteRaw(out, bloom_offset);
  WriteRaw(out, kMagicNumber);

  out.flush();
  return static_cast<bool>(out);
}

SSTable::SSTable(const std::string& path)
    : path_(path), bloom_filter_(std::vector<uint8_t>{1}, 1) {
  std::ifstream in(path_, std::ios::binary);

  in.seekg(-20, std::ios::end);
  uint64_t index_offset = 0, bloom_offset = 0;
  uint32_t magic = 0;
  ReadRaw(in, &index_offset);
  ReadRaw(in, &bloom_offset);
  ReadRaw(in, &magic);

  data_section_end_ = index_offset;

  in.seekg(static_cast<std::streamoff>(index_offset), std::ios::beg);
  uint32_t index_count = 0;
  ReadRaw(in, &index_count);
  sparse_index_.reserve(index_count);
  for (uint32_t i = 0; i < index_count; i++) {
    IndexEntry entry;
    ReadLengthPrefixedString(in, &entry.key);
    ReadRaw(in, &entry.offset);
    sparse_index_.push_back(std::move(entry));
  }

  in.seekg(static_cast<std::streamoff>(bloom_offset), std::ios::beg);
  uint64_t num_bits = 0;
  int32_t num_hashes = 0;
  uint32_t byte_len = 0;
  ReadRaw(in, &num_bits);
  ReadRaw(in, &num_hashes);
  ReadRaw(in, &byte_len);
  std::vector<uint8_t> bits(byte_len);
  if (byte_len > 0) {
    in.read(reinterpret_cast<char*>(bits.data()), byte_len);
  }
  bloom_filter_ = BloomFilter(std::move(bits), num_hashes);
}

SSTableGetResult SSTable::Get(const std::string& key,
                               std::string* out_value) const {
  if (!bloom_filter_.MightContain(key)) {
    return SSTableGetResult::kNotPresent;
  }
  if (sparse_index_.empty()) {
    return SSTableGetResult::kNotPresent;
  }

  auto it = std::upper_bound(
      sparse_index_.begin(), sparse_index_.end(), key,
      [](const std::string& k, const IndexEntry& e) { return k < e.key; });

  if (it == sparse_index_.begin()) {
    return SSTableGetResult::kNotPresent;
  }
  --it;
  uint64_t start_offset = it->offset;

  std::ifstream in(path_, std::ios::binary);
  in.seekg(static_cast<std::streamoff>(start_offset), std::ios::beg);

  while (static_cast<uint64_t>(in.tellg()) < data_section_end_) {
    uint32_t key_len = 0, val_len = 0;
    uint8_t op_byte = 0;
    if (!ReadRaw(in, &key_len)) break;
    if (!ReadRaw(in, &val_len)) break;
    if (!ReadRaw(in, &op_byte)) break;

    std::string record_key(key_len, '\0');
    if (key_len > 0) in.read(&record_key[0], key_len);
    std::string record_value(val_len, '\0');
    if (val_len > 0) in.read(&record_value[0], val_len);
    if (!in) break;

    if (record_key == key) {
      if (static_cast<OpType>(op_byte) == OpType::kDelete) {
        return SSTableGetResult::kDeleted;
      }
      if (out_value != nullptr) {
        *out_value = record_value;
      }
      return SSTableGetResult::kFound;
    }
    if (record_key > key) {
      break;
    }
  }

  return SSTableGetResult::kNotPresent;
}

std::vector<MemtableEntry> SSTable::AllEntries() const {
  std::vector<MemtableEntry> result;

  std::ifstream in(path_, std::ios::binary);
  in.seekg(0, std::ios::beg);

  while (static_cast<uint64_t>(in.tellg()) < data_section_end_) {
    uint32_t key_len = 0, val_len = 0;
    uint8_t op_byte = 0;
    if (!ReadRaw(in, &key_len)) break;
    if (!ReadRaw(in, &val_len)) break;
    if (!ReadRaw(in, &op_byte)) break;

    std::string key(key_len, '\0');
    if (key_len > 0) in.read(&key[0], key_len);
    std::string value(val_len, '\0');
    if (val_len > 0) in.read(&value[0], val_len);
    if (!in) break;

    result.push_back(
        MemtableEntry{std::move(key), std::move(value),
                       static_cast<OpType>(op_byte)});
  }

  return result;
}