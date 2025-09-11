#pragma once

#include "../iterator/iterator.h"
#include <cstdint>
#include <iterator>
#include <memory>
#include <optional>
#include <string>
#include <utility>

namespace my_tiny_lsm {
class Block;
class BlockIterator {
public:
  using iterator_category = std::forward_iterator_tag;
  using difference_type = std::ptrdiff_t;
  using value_type = std::pair<std::string, std::string>;
  using pointer = value_type *;
  using reference = value_type &;

  BlockIterator(std::shared_ptr<Block> b, size_t idx, uint64_t tranc_id);
  BlockIterator(std::shared_ptr<Block> b, const std::string &key,
                uint64_t tranc_id);

  BlockIterator() : block(nullptr), current_index(0), tranc_id_(0) {}

  pointer operator->() const;
  value_type operator*() const;
  BlockIterator &operator++();
  BlockIterator operator++(int) = delete;
  bool operator==(const BlockIterator &other) const;
  bool operator!=(const BlockIterator &other) const;
  bool is_end() const;

private:
  void update_current()const;
  void skip_by_tranc_id();
  std::shared_ptr<Block> block;
  size_t current_index;
  uint64_t tranc_id_;
  mutable std::optional<value_type> cached_value;
};
} // namespace my_tiny_lsm