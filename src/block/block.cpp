#include "../../include/block/block.h"
#include "../../include/block/block_iterator.h"
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <functional>
#include <memory>
#include <optional>
#include <stdexcept>
#include <string>

namespace my_tiny_lsm {
Block::Block(size_t cap) : capacity(cap) {}

std::vector<uint8_t> Block::encode(bool with_hash) {
  size_t total_size = data.size() * sizeof(uint8_t) +
                      offsets.size() * sizeof(uint16_t) + sizeof(uint16_t);
  if (with_hash) {
    total_size += sizeof(uint32_t);
  }
  std::vector<uint8_t> encoded(total_size, 0);

  memcpy(encoded.data(), data.data(), data.size() * sizeof(uint8_t));
  size_t offset_pos = data.size() * sizeof(uint8_t);

  memcpy(encoded.data() + offset_pos, offsets.data(),
         offsets.size() * sizeof(uint16_t));
}

std::shared_ptr<Block> Block::decode(const std::vector<uint8_t> &encoded,
                                     bool with_hash) {
  auto block = std::make_shared<Block>();
  // 1. 安全性检查
  if (with_hash && encoded.size() <= sizeof(uint16_t) + sizeof(uint32_t)) {
    throw std::runtime_error("Encoded data too small");
  }
  uint16_t num_entries;
  size_t num_entries_pos = encoded.size() - sizeof(uint16_t);
  if (with_hash) {
    num_entries_pos -= sizeof(uint32_t);
    auto hash_pos = encoded.size() - sizeof(uint32_t);
    uint32_t hash_value;
    memcpy(&hash_value, encoded.data() + hash_pos, sizeof(uint32_t));

    uint32_t compute_hash = std::hash<std::string_view>{}(
        std::string_view(reinterpret_cast<const char *>(encoded.data()),
                         encoded.size() - sizeof(uint32_t)));
    if (hash_value != compute_hash) {
      throw std::runtime_error("Block hash verification failed");
    }
  }
  memcpy(&num_entries, encoded.data() + num_entries_pos, sizeof(uint16_t));
  size_t required_size = sizeof(uint16_t) + num_entries * sizeof(uint16_t);
  if (encoded.size() < required_size) {
    throw std::runtime_error("Invalid encoded data size");
  }
  size_t offsets_section_start =
      num_entries_pos - num_entries * sizeof(uint16_t);

  block->offsets.resize(num_entries);
  memcpy(block->offsets.data(), encoded.data() + offsets_section_start,
         num_entries * sizeof(uint16_t));

  block->data.reserve(offsets_section_start);
  block->data.assign(encoded.data(), encoded.data() + offsets_section_start);

  return block;
}

std::string Block::get_first_key() {
  if (data.empty() || offsets.empty()) {
    return "";
  }

  // 读取第一个key的长度（前2字节）
  uint16_t key_len;
  memcpy(&key_len, data.data(), sizeof(uint16_t));

  // 读取key
  std::string key(reinterpret_cast<char *>(data.data() + sizeof(uint16_t)),
                  key_len);
  return key;
}

size_t Block::get_offset_at(size_t idx) const {
  if (idx > offsets.size()) {
    throw std::runtime_error("idx out of offsets range");
  }
  return offsets[idx];
}

bool Block::add_entry(const std::string &key, const std::string &value,
                      uint64_t tranc_id, bool force_write) {
  if (!force_write &&
      (cur_size() + key.size() + value.size() + 3 * sizeof(uint16_t) +
           sizeof(uint64_t) >
       capacity) &&
      !offsets.empty()) {
    return false;
  }
  // 计算entry大小：key长度(2B) + key + value长度(2B) + value + tranc_id(8B)
  size_t entry_size = sizeof(uint16_t) + key.size() + sizeof(uint16_t) +
                      value.size() + sizeof(uint64_t);
  size_t offset = data.size();
  data.resize(data.size() + entry_size);
  uint16_t key_size = static_cast<uint16_t>(key.size());
  uint16_t value_size = static_cast<uint16_t>(value.size());

  memcpy(data.data() + offset, &key_size, sizeof(uint16_t));
  memcpy(data.data() + offset + sizeof(uint16_t), key.data(), key.size());
  memcpy(data.data() + offset + sizeof(uint16_t) + key.size(), &value_size,
         sizeof(uint16_t));
  memcpy(data.data() + offset + sizeof(uint16_t) + key.size() +
             sizeof(uint16_t),
         value.data(), value.size());
  memcpy(data.data() + offset + sizeof(uint16_t) + key.size() +
             sizeof(uint16_t) + value.size(),
         &tranc_id, sizeof(uint64_t));
  offsets.push_back(static_cast<uint16_t>(offset));
  return true;
}
// 从指定偏移量获取entry的key
std::string Block::get_key_at(size_t offset) const {
  uint16_t key_len;
  memcpy(&key_len, data.data() + offset, sizeof(uint16_t));
  return std::string(
      reinterpret_cast<const char *>(data.data() + offset + sizeof(uint16_t)),
      key_len);
}

// 从指定偏移量获取entry的value
std::string Block::get_value_at(size_t offset) const {
  // 先获取key长度
  uint16_t key_len;
  memcpy(&key_len, data.data() + offset, sizeof(uint16_t));

  // 计算value长度的位置
  size_t value_len_pos = offset + sizeof(uint16_t) + key_len;
  uint16_t value_len;
  memcpy(&value_len, data.data() + value_len_pos, sizeof(uint16_t));

  // 返回value
  return std::string(reinterpret_cast<const char *>(
                         data.data() + value_len_pos + sizeof(uint16_t)),
                     value_len);
}

uint64_t Block::get_tranc_id_at(size_t offset) const {
  // 先获取key长度
  uint16_t key_len;
  memcpy(&key_len, data.data() + offset, sizeof(uint16_t));

  // 计算value长度的位置
  size_t value_len_pos = offset + sizeof(uint16_t) + key_len;
  uint16_t value_len;
  memcpy(&value_len, data.data() + value_len_pos, sizeof(uint16_t));

  // 计算事务id的位置
  size_t tranc_id_pos = value_len_pos + sizeof(uint16_t) + value_len;
  uint64_t tranc_id;
  memcpy(&tranc_id, data.data() + tranc_id_pos, sizeof(uint64_t));
  return tranc_id;
}

// 比较指定偏移量处的key与目标key
int Block::compare_key_at(size_t offset, const std::string &target) const {
  std::string key = get_key_at(offset);
  return key.compare(target);
}

int Block::adjust_idx_by_tranc_id(size_t idx, uint64_t tranc_id) {
  if (idx >= offsets.size()) {
    return -1;
  }
  auto target_key = get_key_at(offsets[idx]);
  if (tranc_id != 0) {
    auto current_tranc_id = get_tranc_id_at(offsets[idx]);
    if (current_tranc_id <= tranc_id) {
      size_t prev_idx = idx;
      while (prev_idx > 0 && is_same_key(prev_idx - 1, target_key)) {
        --prev_idx;
        auto new_tranc_id = get_tranc_id_at(offsets[prev_idx]);
        if (new_tranc_id > tranc_id) {
          return prev_idx + 1;
        }
      }
      return prev_idx;
    } else {
      size_t next_idx = idx + 1;
      while (next_idx < offsets.size() && is_same_key(next_idx, target_key)) {
        auto new_tranc_id = get_tranc_id_at(offsets[next_idx]);
        if (new_tranc_id <= tranc_id) {
          return next_idx;
        }
        ++next_idx;
      }
      return -1;
    }
  } else {
    size_t prev_idx = idx;
    while (prev_idx > 0 && is_same_key(prev_idx - 1, target_key)) {
      --prev_idx;
    }
    return prev_idx;
  }
}

bool Block::is_same_key(size_t idx, const std::string &target_key) const {
  if (idx >= offsets.size()) {
    return false; // 索引超出范围
  }
  return get_key_at(offsets[idx]) == target_key;
}
// 使用二分查找获取value
// 要求在插入数据时有序插入
std::optional<std::string> Block::get_value_binary(const std::string &key,
                                                   uint64_t tranc_id) {
  auto idx = get_index_binary(key, tranc_id);
  if (!idx.has_value()) {
    return std::nullopt;
  }

  return get_value_at(offsets[*idx]);
}

std::optional<size_t> Block::get_index_binary(const std::string &key,
                                              uint64_t tranc_id) {
  if (offsets.empty()) {
    return std::nullopt;
  }
  int left = 0;
  int right = offsets.size() - 1;
  while (left <= right) {
    int mid = (left + right) / 2;
    size_t mid_offset = offsets[mid];
    int cmp = compare_key_at(mid_offset, key);

    if (cmp == 0) {
      auto new_mid = adjust_idx_by_tranc_id(mid, tranc_id);
      if (new_mid == -1) {
        return std::nullopt;
      }
      return static_cast<size_t>(new_mid);
    } else if (cmp < 0) {
      left = mid + 1;

    } else {
      right = mid - 1;
    }
  }
  return std::nullopt;
}
Block::Entry Block::get_entry_at(size_t offset) const {
  Entry entry;
  entry.key = get_key_at(offset);
  entry.value = get_value_at(offset);
  entry.tranc_id = get_tranc_id_at(offset);
  return entry;
}

size_t Block::size() const { return offsets.size(); }

size_t Block::cur_size() const {
  return data.size() + offsets.size() * sizeof(uint16_t) + sizeof(uint16_t);
}

bool Block::is_empty() const { return offsets.empty(); }

BlockIterator Block::begin(uint64_t tranc_id) {
  return BlockIterator(shared_from_this(), 0, tranc_id);
}

std::optional<
    std::pair<std::shared_ptr<BlockIterator>, std::shared_ptr<BlockIterator>>>
Block::iters_preffix(uint64_t tranc_id, const std::string &preffix) {
  auto func = [&preffix](const std::string &key) {
    return -key.compare(0, preffix.size(), preffix);
  };
  return get_monotony_predicate_iters(tranc_id, func);
}

BlockIterator Block::end() {
  return BlockIterator(shared_from_this(), offsets.size(), 0);
}

// 返回第一个满足谓词的位置和最后一个满足谓词的位置
// 如果不存在, 范围nullptr
// 谓词作用于key, 且保证满足谓词的结果只在一段连续的区间内, 例如前缀匹配的谓词
// 返回的区间是闭区间, 开区间需要手动对返回值自增
// predicate返回值:
//   0: 满足谓词
//   >0: 不满足谓词, 需要向右移动
//   <0: 不满足谓词, 需要向左移动
std::optional<
    std::pair<std::shared_ptr<BlockIterator>, std::shared_ptr<BlockIterator>>>
Block::get_monotony_predicate_iters(
    uint64_t tranc_id, std::function<int(const std::string &)> predicate) {
  if (offsets.empty()) {
    return std::nullopt;
  }

  int left = 0;
  int right = offsets.size() - 1;
  int first = -1;
  while (left <= right) {
    int mid = (left + right) / 2;
    size_t mid_offset = offsets[mid];
    auto mid_key = get_key_at(mid_offset);
    int direction = predicate(mid_key);
    if (direction <= 0) {
      right = mid - 1;
    } else {
      left = mid + 1;
    }
  }
  if (left >= offsets.size() || predicate(get_key_at(offsets[left]))) {
    return std::nullopt;
  }
  first = left;
  // 第二次二分查找，找到最后一个满足谓词的位置
  int last = -1;
  right = offsets.size() - 1;
  while (left <= right) {
    int mid = left + (right - left) / 2;
    size_t mid_offset = offsets[mid];
    auto mid_key = get_key_at(mid_offset);
    int direction = predicate(mid_key);
    if (direction < 0) {
      right = mid - 1;
    } else
      left = mid + 1;
  }
  last = left - 1;
  auto it_begin =
      std::make_shared<BlockIterator>(shared_from_this(), first, tranc_id);
  auto it_end =
      std::make_shared<BlockIterator>(shared_from_this(), last + 1, tranc_id);
  return std::make_optional(std::make_pair(it_begin, it_end));
}
} // namespace my_tiny_lsm