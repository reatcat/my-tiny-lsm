#pragma once

#include <cstddef>
#include <cstdint>
#include <string>
#include <vector>

namespace my_tiny_lsm {

class BlockMeta {
public:
  size_t offset;
  std::string first_key;
  std::string last_key;
  //   序列化和反序列化
  static void encode_meta_to_slice(std::vector<BlockMeta> &meta_entries,
                                   std::vector<uint8_t> &metadata);
  static std::vector<BlockMeta>
  decode_meta_from_slice(const std::vector<uint8_t> &metadata);
  BlockMeta();
  BlockMeta(size_t off, const std::string &first, const std::string &last);
};
} // namespace my_tiny_lsm