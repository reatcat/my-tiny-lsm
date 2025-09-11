#include "../../include/block/blockmeta.h"
#include <cstring>
#include <functional>
#include <stdexcept>

namespace my_tiny_lsm {

BlockMeta::BlockMeta() : offset(0), first_key(""), last_key("") {}

BlockMeta::BlockMeta(size_t off, const std::string &first,
                     const std::string &last)
    : offset(off), first_key(first), last_key(last) {}

void BlockMeta::encode_meta_to_slice(std::vector<BlockMeta> &meta_entries,
                                     std::vector<uint8_t> &metadata) {
  uint32_t num_blocks = meta_entries.size();
  size_t total_size = sizeof(uint32_t);
  for(const auto &entry : meta_entries) {
    total_size += sizeof(size_t); // offset
    total_size += sizeof(uint16_t) + entry.first_key.size(); // first_key
    total_size += sizeof(uint16_t) + entry.last_key.size();  // last_key
  }
  total_size += sizeof(uint32_t);
  metadata.resize(total_size);
  uint8_t *ptr = metadata.data();
  memcpy(ptr,&num_blocks,sizeof(uint32_t));
  ptr += sizeof(uint32_t);
  for(const auto &entry : meta_entries) {
    uint32_t offset = static_cast<uint32_t>(entry.offset);
    memcpy(ptr,&offset,sizeof(uint32_t));
    ptr += sizeof(uint32_t);
    uint16_t first_key_size = static_cast<uint16_t>(entry.first_key.size());
    memcpy(ptr,&first_key_size,sizeof(uint16_t));
    ptr += sizeof(uint16_t);
    memcpy(ptr,entry.first_key.data(),first_key_size);
    ptr += first_key_size; 

    uint16_t last_key_size = static_cast<uint16_t>(entry.last_key.size());
    memcpy(ptr,&last_key_size,sizeof(uint16_t));
    ptr += sizeof(uint16_t);
    memcpy(ptr,entry.last_key.data(),last_key_size);
    ptr += last_key_size;
  }
  const uint8_t *data_begin = metadata.data() + sizeof(uint32_t);
  const uint8_t *data_end = ptr;

  size_t data_len = data_end - data_begin;
  uint32_t checksum = std::hash<std::string_view>()(
      std::string_view(reinterpret_cast<const char *>(data_begin), data_len));
    memcpy(ptr,&checksum,sizeof(uint32_t));
}

std::vector<BlockMeta>
BlockMeta::decode_meta_from_slice(const std::vector<uint8_t> &metadata) {
  std::vector<BlockMeta> meta_entries;

  // 1. 验证最小长度
  if (metadata.size() < sizeof(uint32_t) * 2) { // 至少要有num_entries和hash
    throw std::runtime_error("Invalid metadata size");
  }

  // 2. 读取元素个数
  uint32_t num_entries;
  const uint8_t *ptr = metadata.data();
  memcpy(&num_entries, ptr, sizeof(uint32_t));
  ptr += sizeof(uint32_t);

  // 3. 读取entries
  for (uint32_t i = 0; i < num_entries; ++i) {
    BlockMeta meta;

    // 读取 offset
    uint32_t offset32;
    memcpy(&offset32, ptr, sizeof(uint32_t));
    meta.offset = offset32;
    ptr += sizeof(uint32_t);

    // 读取 first_key
    uint16_t first_key_len;
    memcpy(&first_key_len, ptr, sizeof(uint16_t));
    ptr += sizeof(uint16_t);
    meta.first_key.assign(reinterpret_cast<const char *>(ptr), first_key_len);
    ptr += first_key_len;

    // 读取 last_key
    uint16_t last_key_len;
    memcpy(&last_key_len, ptr, sizeof(uint16_t));
    ptr += sizeof(uint16_t);
    meta.last_key.assign(reinterpret_cast<const char *>(ptr), last_key_len);
    ptr += last_key_len;

    meta_entries.push_back(meta);
  }

  // 4. 验证hash
  uint32_t stored_hash;
  memcpy(&stored_hash, ptr, sizeof(uint32_t));

  const uint8_t *data_start = metadata.data() + sizeof(uint32_t);
  const uint8_t *data_end = ptr;
  size_t data_len = data_end - data_start;

  // 使用与编码时相同的 std::hash 计算哈希值
  uint32_t computed_hash = std::hash<std::string_view>{}(
      std::string_view(reinterpret_cast<const char *>(data_start), data_len));

  if (stored_hash != computed_hash) {
    throw std::runtime_error("Metadata hash mismatch");
  }

  return meta_entries;
}
} // namespace my_tiny_lsm