#pragma once

#include <cstddef>
#include <cstdint>
#include <functional>
#include <memory>
#include <optional>
#include <string>
#include <utility>
#include <vector>

namespace my_tiny_lsm {

class BlockIterator;

class Block : public std::enable_shared_from_this<Block> {
  friend BlockIterator;

private:
  std::vector<uint8_t> data;
  std::vector<uint16_t> offsets;
  size_t capacity;
  struct Entry {
    std::string key;
    std::string value;
    uint64_t tranc_id; // 事务 id
  };
  Entry get_entry_at(size_t offset) const;
  std::string get_key_at(size_t offset) const;
  std::string get_value_at(size_t offset) const;
  uint64_t get_tranc_id_at(size_t offset) const;

  int compare_key_at(size_t offset, const std::string &target) const;
  int adjust_idx_by_tranc_id(size_t idx, uint64_t tranc_id);

  bool is_same_key(size_t idx, const std::string &target_key) const;

public:
  Block() = default;
  Block(size_t cap);
  std::vector<uint8_t> encode(bool with_hash = true);
  static std::shared_ptr<Block> decode(const std::vector<uint8_t> &encoded,
                                       bool with_hash = true);
  std::string get_first_key();
  size_t get_offset_at(size_t idx) const;
  bool add_entry(const std::string &key, const std::string &value,
                 uint64_t tranc_id, bool force_write);
  std::optional<std::string> get_value_binary(const std::string &key,
                                              uint64_t tranc_id);
  size_t size() const;
  size_t cur_size() const;
  bool is_empty() const;
  std::optional<size_t> get_index_binary(const std::string &key,
                                         uint64_t tranc_id);

  // 按照谓词返回迭代器, 左闭右开
  std::optional<
      std::pair<std::shared_ptr<BlockIterator>, std::shared_ptr<BlockIterator>>>
  get_monotony_predicate_iters(uint64_t tranc_id,
                               std::function<int(const std::string &)> func);

  BlockIterator begin(uint64_t tranc_id);

  std::optional<
      std::pair<std::shared_ptr<BlockIterator>, std::shared_ptr<BlockIterator>>>
  iters_preffix(uint64_t tranc_id, const std::string &preffix);

  BlockIterator end();
};
} // namespace my_tiny_lsm