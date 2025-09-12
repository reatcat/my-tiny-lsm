#pragma once

#include "../block/block.h"
#include "../block/block_cache.h"
#include "../block/blockmeta.h"
#include "../utils/bloom_filter.h"
#include "../utils/files.h"
#include <cstddef>
#include <cstdint>
#include <memory>
#include <string>
#include <utility>
#include <vector>

namespace my_tiny_lsm {

class SSTableIterator;

class SST : public std::enable_shared_from_this<SST> {
  friend class SSTBuilder;
  friend std::optional<std::pair<SSTableIterator, SSTableIterator>>
  sst_iters_monotony_predicate(std::shared_ptr<SST> sst, uint64_t tranc_id,
                               std::function<int(const std::string &)> predicate);

private:
  FileObj file;
  std::vector<BlockMeta> meta_entries;
  uint32_t bloom_offset;
  uint32_t meta_block_offset;
  size_t sst_id;
  std::string first_key;
  std::string last_key;
  std::shared_ptr<BloomFilter> bloom_filter;
  std::shared_ptr<BlockCache> block_cache;
  uint64_t min_tranc_id;
  uint64_t max_tranc_id;

public:
  static std::shared_ptr<SST> open(size_t sst_id, FileObj file,
                                   std::shared_ptr<BlockCache> block_cache);
  void del_sst();

  std::shared_ptr<Block> read_block(size_t block_idx);
  size_t find_block_idx(const std::string &key);
  SSTableIterator get(const std::string &key, uint64_t tranc_id);
  size_t num_blocks() const;
    // 返回sst的首key
  std::string get_first_key() const;

  // 返回sst的尾key
  std::string get_last_key() const;

  // 返回sst的大小
  size_t sst_size() const;

  // 返回sst的id
  size_t get_sst_id() const;

  std::optional<std::pair<SSTableIterator, SSTableIterator>>
  iters_monotony_predicate(std::function<bool(const std::string &)> predicate);

  SSTableIterator begin(uint64_t tranc_id);
  SSTableIterator end();

  std::pair<uint64_t, uint64_t> get_tranc_id_range() const;
};
class SSTBuilder {
private:
  Block block;
  std::string first_key;
  std::string last_key;
  std::vector<BlockMeta> meta_entries;
  std::vector<uint8_t> data;
  size_t block_size;
  std::shared_ptr<BloomFilter> bloom_filter;
  uint64_t min_tranc_id;
  uint64_t max_tranc_id;

public:
  SSTBuilder(size_t block_size, bool has_bloom);
  void add(const std::string &key, const std::string &value, uint64_t tranc_id);
  size_t estimated_size() const;
  void finish_block();
  std::shared_ptr<SST> build(size_t sst_id, const std::string &path,
                             std::shared_ptr<BlockCache> block_cache);
};

} // namespace my_tiny_lsm