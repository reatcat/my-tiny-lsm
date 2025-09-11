#pragma once

#include "block.h"
#include <cstdint>
#include <list>
#include <memory>
#include <mutex>
#include <sys/types.h>
#include <unordered_map>
#include <utility>
#include <vector>

namespace my_tiny_lsm {

struct CacheItem {
  int sst_id;
  int block_id;
  std::shared_ptr<Block> block_ptr;
  uint64_t access_count;
};

struct pair_hash {
  template <class T1, class T2>
  std::size_t operator()(const std::pair<T1, T2> &pair) const {
    auto hash1 = std::hash<T1>{}(pair.first);
    auto hash2 = std::hash<T2>{}(pair.second);
    return hash1 ^ hash2;
  }
};

struct pair_equal {
  template <class T1, class T2>
  bool operator()(const std::pair<T1, T2> &p1,
                  const std::pair<T1, T2> &p2) const {
    return p1.first == p2.first && p1.second == p2.second;
  }
};

class BlockCache {
public:
  BlockCache(size_t capacity, size_t k);
  ~BlockCache();
  std::shared_ptr<Block> get(int sst_id, int block_id);
  void put(int stt_id, int block_id, std::shared_ptr<Block> block);
  double hit_rate() const;

private:
  size_t capacity_;
  size_t k_; // 划分两部分的阈值
  mutable std::mutex mutex_;

  std::list<CacheItem> cache_list_greater_k;
  std::list<CacheItem> cache_list_less_k;
  //   键为 (sst_id, block_id) 的组合 ，值为指向 cache_list 中对应 CacheItem
  //   的迭代器
  std::unordered_map<std::pair<int, int>, std::list<CacheItem>::iterator,
                     pair_hash, pair_equal>
      cache_map_;

  void update_access_time(std::list<CacheItem>::iterator it);

  mutable size_t total_requests_ = 0;
  mutable size_t hit_requests_ = 0;
};
} // namespace my_tiny_lsm