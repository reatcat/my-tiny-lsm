#include "../../include/block/block_iterator.h"
#include "../../include/block/block.h"
#include <cstdint>
#include <memory>
#include <stdexcept>

class Block;

namespace my_tiny_lsm {
BlockIterator::BlockIterator(std::shared_ptr<Block> b, size_t idx,
                             uint64_t tranc_id)
    : block(b), current_index(idx), tranc_id_(tranc_id),
      cached_value(std::nullopt) {
  skip_by_tranc_id();
}

BlockIterator::BlockIterator(std::shared_ptr<Block> b, const std::string &key,
                             uint64_t tranc_id)
    : block(b), tranc_id_(tranc_id), cached_value(std::nullopt) {
  auto key_idx_opt = block->get_index_binary(key, tranc_id);
  if (key_idx_opt.has_value()) {
    current_index = key_idx_opt.value();
  } else {
    current_index = block->offsets.size(); // 设置为end()
  }
}
BlockIterator::pointer BlockIterator::operator->() const {
  update_current();
  return &(*cached_value);
}

BlockIterator::value_type BlockIterator::operator*() const {
  if (!block || current_index >= block->offsets.size()) {
    throw std::out_of_range("Dereferencing end iterator or invalid iterator");
  }

  if (!cached_value.has_value()) {
    size_t offset = block->get_offset_at(current_index);
    cached_value =
        std::make_pair(block->get_key_at(offset), block->get_value_at(offset));
  }
  return *cached_value;
}

bool BlockIterator::operator==(const BlockIterator &other) const {
  if (block == nullptr && other.block == nullptr) {
    return true;
  }
  if (block == nullptr || other.block == nullptr) {
    return false;
  }
  auto cmp = block == other.block && current_index == other.current_index;
  return cmp;
}

bool BlockIterator::operator!=(const BlockIterator &other) const {
  return !(*this == other);
}

BlockIterator::value_type BlockIterator::operator*() const {
  if (!block || current_index >= block->size()) {
    throw std::out_of_range("Iterator out of range");
  }

  // 使用缓存避免重复解析
  if (!cached_value.has_value()) {
    size_t offset = block->get_offset_at(current_index);
    cached_value =
        std::make_pair(block->get_key_at(offset), block->get_value_at(offset));
  }
  return *cached_value;
}

bool BlockIterator::is_end() const {
  return current_index == block->offsets.size();
}

BlockIterator &BlockIterator::operator++() {
    if(block && current_index < block->size()) {
        auto prev_index = current_index;
        auto prev_offset = block->get_offset_at(prev_index);
        auto prev_entry = block->get_entry_at(prev_offset);
        ++current_index;
        while(block && current_index < block->size()) {
            auto curr_offset = block->get_offset_at(current_index);
            auto curr_entry = block->get_entry_at(curr_offset);
            if(curr_entry.key != prev_entry.key) {
                break; // 找到不同的key，停止跳过
            }
            ++current_index;
        }
        skip_by_tranc_id();
    }
    return *this;
}

void BlockIterator::update_current() const {
    if(!cached_value && current_index < block->size()) {
        size_t offset = block->get_offset_at(current_index);
        cached_value = std::make_pair(block->get_key_at(offset), block->get_value_at(offset));
    }
}

void BlockIterator::skip_by_tranc_id() {
    if(tranc_id_ == 0) return; // tranc_id_ 为 0 时不过滤

    while(current_index < block->size()) {
        size_t offset = block->get_offset_at(current_index);
        uint64_t entry_tranc_id = block->get_tranc_id_at(offset);
        if(entry_tranc_id <= tranc_id_) {
            break; // 找到符合条件的 entry，停止跳过
        }
        ++current_index;
    }
}

} // namespace my_tiny_lsm