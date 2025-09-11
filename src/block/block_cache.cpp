#include "../../include/block/block_cache.h"
#include "../../include/block/block.h"
#include <chrono>
#include <list>
#include <memory>
#include <mutex>
#include <unordered_map>

namespace my_tiny_lsm {

BlockCache::BlockCache(size_t capacity, size_t k)
    : capacity_(capacity), k_(k) {}

BlockCache::~BlockCache() = default;

std::shared_ptr<Block> BlockCache::get(int sst_id, int block_id) {
  std::lock_guard<std::mutex> lock(mutex_);
  total_requests_++;
  auto key = std::make_pair(sst_id, block_id);
  auto it = cache_map_.find(key);
  if (it == cache_map_.end()) {
    return nullptr; // 未命中
  }
  hit_requests_++;
  update_access_time(it->second);
  return it->second->block_ptr;
}

void BlockCache::put(int sst_id, int block_id, std::shared_ptr<Block> block) {
  std::lock_guard<std::mutex> lock(mutex_);
  auto key = std::make_pair(sst_id, block_id);

  auto it = cache_map_.find(key);

  if (it != cache_map_.end()) {
    // 如果已经存在，更新内容并调整位置
    it->second->block_ptr = block;
    update_access_time(it->second);
    return;
  }
  if (cache_map_.size() >= capacity_) {
    if (!cache_list_less_k.empty()) {
      cache_map_.erase(std::make_pair(cache_list_less_k.back().sst_id,
                                      cache_list_less_k.back().block_id));
      cache_list_less_k.pop_back();
    } else {
      cache_map_.erase(std::make_pair(cache_list_greater_k.back().sst_id,
                                      cache_list_greater_k.back().block_id));
      cache_list_greater_k.pop_back();
    }
  }
  // 插入新元素, 初始访问时间为当前时间戳, 放在链表头部
  CacheItem item = {sst_id, block_id, block, 1};
  cache_list_less_k.push_front(item);
  cache_map_[key] = cache_list_less_k.begin();
}
void BlockCache::update_access_time(std::list<CacheItem>::iterator it) {
  ++it->access_count;
  if (it->access_count < k_) {
    cache_list_less_k.splice(cache_list_less_k.begin(), cache_list_less_k, it);
  } else if (it->access_count == k_) {
    auto item = *it;
    cache_list_less_k.erase(it);
    cache_list_greater_k.push_front(item);
    cache_map_[std::make_pair(item.sst_id, item.block_id)] =
        cache_list_greater_k.begin();

  } else {
    cache_list_greater_k.splice(cache_list_greater_k.begin(),
                                cache_list_greater_k, it);
  }
}
double BlockCache::hit_rate() const {
  std::lock_guard<std::mutex> lock(mutex_);
  return total_requests_ == 0
             ? 0.0
             : static_cast<double>(hit_requests_) / total_requests_;
}

} // namespace my_tiny_lsm