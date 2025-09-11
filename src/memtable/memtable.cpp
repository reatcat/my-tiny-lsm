#include "../../include/memtable/memtable.h"
#include "../../include/config/config.h"
#include "../../include/consts.h"
#include "../../include/iterator/iterator.h"
#include "../../include/skiplist/skiplist.h"
#include "../../include/sst/sst.h"
#include "spdlog/spdlog.h"
#include <algorithm>
#include <cstddef>
#include <memory>
#include <mutex>
#include <optional>
#include <shared_mutex>
#include <sys/types.h>
#include <utility>
#include <vector>

namespace my_tiny_lsm {

class BlockCache;

MemTable::MemTable() : frozen_size_(0) {
  current_table_ = std::make_shared<Skiplist>();
}
MemTable::~MemTable() = default;
void MemTable::put_(const std::string &key, const std::string &value,
                    uint64_t transaction_id) {

  current_table_->put(key, value, transaction_id);
}

void MemTable::put(const std::string &key, const std::string &value,
                   uint64_t transaction_id) {
  std::unique_lock<std::shared_mutex> lock(current_mtx);
  put_(key, value, transaction_id);
  if (current_table_->get_size() >= LSM_PER_MEM_SIZE_LIMIT) {
    std::unique_lock<std::shared_mutex> freeze_lock(frozen_mtx);
    frozen_cur_table_();
  }
}

void MemTable::put_batch(
    const std::vector<std::pair<std::string, std::string>> &kv,
    uint64_t transaction_id) {
  std::unique_lock<std::shared_mutex> lock(current_mtx);
  for (auto &[key, value] : kv) {
    put_(key, value, transaction_id);
  }
  if (current_table_->get_size() >= LSM_PER_MEM_SIZE_LIMIT) {
    std::unique_lock<std::shared_mutex> freeze_lock(frozen_mtx);
    frozen_cur_table_();
  }
}

SkiplistIterator MemTable::cur_get_(const std::string &key,
                                    uint64_t transaction_id) {
  auto result = current_table_->get(key, transaction_id);
  if (result.is_valid()) {
    return result;
  }
  return SkiplistIterator();
}
SkiplistIterator MemTable::frozen_get_(const std::string &key,
                                       uint64_t transaction_id) {
  // read dont change frozenlist ,dont need lock
  // std::shared_lock<std::shared_mutex> lock(frozen_mtx);
  for (auto &table : frozen_tables_) {
    auto result = table->get(key, transaction_id);
    if (result.is_valid()) {
      return result;
    }
  }
  return SkiplistIterator();
}
// 外部获取，需要上锁
SkiplistIterator MemTable::get(const std::string &key,
                               uint64_t transaction_id) {
  std::shared_lock<std::shared_mutex> lock(current_mtx);
  auto cur_res = cur_get_(key, transaction_id);
  if (cur_res.is_valid()) {
    return cur_res;
  }
  lock.unlock();
  std::shared_lock<std::shared_mutex> freeze_lock(frozen_mtx);
  return frozen_get_(key, transaction_id);
}
SkiplistIterator MemTable::get_(const std::string &key,
                                uint64_t transaction_id) {
  auto cur_res = cur_get_(key, transaction_id);
  return cur_res.is_valid() ? cur_res : frozen_get_(key, transaction_id);
}

std::vector<
    std::pair<std::string, std::optional<std::pair<std::string, uint64_t>>>>
MemTable::get_batch(const std::vector<std::string> &keys,
                    uint64_t transaction_id) {

  std::vector<
      std::pair<std::string, std::optional<std::pair<std::string, uint64_t>>>>
      results;
  results.reserve(keys.size());

  // 步骤 1: 首先在活跃表中查找，并创建初始的结果向量。
  // 对于未找到的键，其值将是 std::nullopt。

  bool all_found = true;
  {
    std::shared_lock<std::shared_mutex> lock(current_mtx);
    for (const auto &key : keys) {
      auto cur_res = cur_get_(key, transaction_id);
      if (cur_res.is_valid()) {
        results.emplace_back(key, std::make_pair(cur_res.get_value(),
                                                 cur_res.get_transaction_id()));
      } else {
        all_found = false;
        results.emplace_back(key, std::nullopt);
      }
    }
  } // cur_mtx 的读锁在此处被释放

  if (all_found) {
    // 在返回前，处理删除标记（值为空字符串的键）
    for (auto &result_pair : results) {
      if (result_pair.second.has_value() && result_pair.second->first.empty()) {
        result_pair.second = std::nullopt;
      }
    }
    return results;
  }

  // 步骤 3: 对于尚未找到的键，在冻结表中继续查找。
  {
    std::shared_lock<std::shared_mutex> freeze_lock(frozen_mtx);
    // 直接遍历结果向量，只处理那些值还是 nullopt 的条目
    for (auto &result_pair : results) {
      // 如果这个键已经有值了，就跳过
      if (result_pair.second.has_value()) {
        continue;
      }

      // 否则，在冻结表中查找这个键
      auto frozen_res = frozen_get_(result_pair.first, transaction_id);
      if (frozen_res.is_valid()) {
        // 如果找到了，就更新结果中的 optional
        result_pair.second = std::make_pair(frozen_res.get_value(),
                                            frozen_res.get_transaction_id());
      }
      // 如果在冻结表中也没找到，它的值将保持为 std::nullopt，无需任何操作。
    }
  } // frozen_mtx 的读锁在此处被释放

  // 步骤 4: 统一处理删除标记（Tombstone）。
  // 这使得API的行为更清晰：一个被删除的键对于GET操作来说，等同于未找到。
  for (auto &result_pair : results) {
    if (result_pair.second.has_value() && result_pair.second->first.empty()) {
      result_pair.second = std::nullopt;
    }
  }

  return results;
}

void MemTable::remove_(const std::string &key, uint64_t transaction_id) {
  current_table_->put(key, "", transaction_id);
}

void MemTable::remove(const std::string &key, uint64_t transacton_id) {
  std::unique_lock<std::shared_mutex> lock(current_mtx);
  remove_(key, transacton_id);
  if (current_table_->get_size() >= LSM_PER_MEM_SIZE_LIMIT) {
    std::unique_lock<std::shared_mutex> freeze_lock(frozen_mtx);
    frozen_cur_table_();
  }
}

void MemTable::remove_batch(const std::vector<std::string> &keys,
                            uint64_t transaction_id) {
  std::unique_lock<std::shared_mutex> lock(current_mtx);
  for (auto &key : keys) {
    remove_(key, transaction_id);
  }
  if (current_table_->get_size() >= LSM_PER_MEM_SIZE_LIMIT) {
    std::unique_lock<std::shared_mutex> freeze_lock(frozen_mtx);
    frozen_cur_table_();
  }
}

void MemTable::clear() {
  std::unique_lock<std::shared_mutex> lock1(current_mtx);
  std::unique_lock<std::shared_mutex> lock2(frozen_mtx);
  current_table_->clear();
  frozen_tables_.clear();
}

std::shared_ptr<SST>
MemTable::flush_last(SSTBuilder &builder, std::string &sst_path, size_t sst_id,
                     std::vector<uint64_t> &flush_transaction_ids,
                     std::shared_ptr<BlockCache> block_cache) {
  std::unique_lock<std::shared_mutex> lock(frozen_mtx);
  if (frozen_tables_.empty()) {
    // frozen_tables_.push_front(current_table_);
    // frozen_size_ += current_table_->get_size();
    // current_table_ = std::make_shared<Skiplist>();
    frozen_cur_table_();
    return nullptr;
  }
  uint64_t max_tranc_id = 0;
  uint64_t min_tranc_id = UINT64_MAX;
  std::shared_ptr<Skiplist> table = frozen_tables_.back();
  frozen_tables_.pop_back();
  frozen_size_ -= table->get_size();
  std::vector<std::tuple<std::string, std::string, uint64_t>> data =
      table->flush();
  for (auto &[key, value, tranc_id] : data) {
    if (key == "" && value == "") {
      flush_transaction_ids.push_back(tranc_id);
    }
    max_tranc_id = std::max(max_tranc_id, tranc_id);
    min_tranc_id = std::min(min_tranc_id, tranc_id);
    builder.add(key, value, tranc_id);
  }
  auto sst = builder.build(sst_id, sst_path, block_cache);

  return sst;
}

void MemTable::frozen_cur_table_() {
  frozen_size_ += current_table_->get_size();
  frozen_tables_.push_front(current_table_);
  current_table_ = std::make_shared<Skiplist>();
}

size_t MemTable::get_cur_size() { return current_table_->get_size(); }
size_t MemTable::get_frozen_size() { return frozen_size_; }
size_t MemTable::get_total_size() {
  std::shared_lock<std::shared_mutex> lock1(current_mtx);
  std::shared_lock<std::shared_mutex> lock2(frozen_mtx);
  return current_table_->get_size() + frozen_size_;
}

HeapIterator MemTable::begin(uint64_t tranc_id) {
  std::shared_lock<std::shared_mutex> slock1(current_mtx);
  std::shared_lock<std::shared_mutex> slock2(frozen_mtx);
  std::vector<SearchItem> item_vec;

  for (auto iter = current_table_->begin(); iter != current_table_->end();
       ++iter) {
    if (tranc_id != 0 && iter.get_transaction_id() > tranc_id) {
      continue;
    }
    item_vec.emplace_back(iter.get_key(), iter.get_value(), 0, 0,
                          iter.get_transaction_id());
  }

  int table_idx = 1;
  for (auto ft = frozen_tables_.begin(); ft != frozen_tables_.end(); ft++) {
    auto table = *ft;
    for (auto iter = table->begin(); iter != table->end(); ++iter) {
      if (tranc_id != 0 && iter.get_transaction_id() > tranc_id) {
        continue;
      }
      item_vec.emplace_back(iter.get_key(), iter.get_value(), table_idx, 0,
                            iter.get_transaction_id());
    }
    table_idx++;
  }

  return HeapIterator(item_vec, tranc_id);
}
HeapIterator MemTable::end() {
  std::shared_lock<std::shared_mutex> slock1(current_mtx);
  std::shared_lock<std::shared_mutex> slock2(frozen_mtx);
  return HeapIterator();
}

HeapIterator MemTable::iters_preffix(const std::string &preffix,
                                     uint64_t tranc_id) {
  std::shared_lock<std::shared_mutex> slock1(current_mtx);
  std::shared_lock<std::shared_mutex> slock2(frozen_mtx);
  std::vector<SearchItem> item_vec;

  for (auto iter = current_table_->begin_preffix(preffix);
       iter != current_table_->end_preffix(preffix); ++iter) {
    if (tranc_id != 0 && iter.get_transaction_id() > tranc_id) {
      continue;
    }
    if (!item_vec.empty() && item_vec.back().key_ == iter.get_key()) {
      continue;
    }
    item_vec.emplace_back(iter.get_key(), iter.get_value(), 0, 0,
                          iter.get_transaction_id());
  }

  int table_idx = 1;
  for (auto ft = frozen_tables_.begin(); ft != frozen_tables_.end(); ft++) {
    auto table = *ft;
    for (auto iter = table->begin_preffix(preffix);
         iter != table->end_preffix(preffix); ++iter) {
      if (tranc_id != 0 && iter.get_transaction_id() > tranc_id) {
        continue;
      }
      if (!item_vec.empty() && item_vec.back().key_ == iter.get_key()) {
        continue;
      }
      item_vec.emplace_back(iter.get_key(), iter.get_value(), table_idx, 0,
                            iter.get_transaction_id());
    }
    table_idx++;
  }

  return HeapIterator(item_vec, tranc_id);
}

std::optional<std::pair<HeapIterator, HeapIterator>>
MemTable::iters_monotony_predicate(
    uint64_t tranc_id, std::function<int(const std::string &)> predicate) {
  std::shared_lock<std::shared_mutex> slock1(current_mtx);
  std::shared_lock<std::shared_mutex> slock2(frozen_mtx);

  std::vector<SearchItem> item_vec;
  auto current_res = current_table_->iters_monotony_predicate(predicate);
  if(current_res.has_value()) {

    auto [begin_it, end_it] = current_res.value();
    for(auto iter = begin_it; iter != end_it; ++iter) {
      if (tranc_id != 0 && iter.get_transaction_id() > tranc_id) {
        continue;
      }
      if(!item_vec.empty() && item_vec.back().key_ == iter.get_key()) {
        continue;
      }
      item_vec.emplace_back(iter.get_key(), iter.get_value(), 0, 0,
                            iter.get_transaction_id());
    }
  }
  int table_idx = 1;
  for (auto ft = frozen_tables_.begin(); ft != frozen_tables_.end(); ft++) {
    auto table = *ft;
    auto frozen_res = table->iters_monotony_predicate(predicate);
    if(frozen_res.has_value()) {    
      auto [begin_it, end_it] = frozen_res.value();
      for(auto iter = begin_it; iter != end_it; ++iter) {
        if (tranc_id != 0 && iter.get_transaction_id() > tranc_id) {
          continue;
        }
        if(!item_vec.empty() && item_vec.back().key_ == iter.get_key()) {
          continue;
        }
        item_vec.emplace_back(iter.get_key(), iter.get_value(), table_idx, 0,
                              iter.get_transaction_id());
      }
    }
    table_idx++;  
  }

  if(item_vec.empty()) {
    return std::nullopt;
  }
  return std::make_optional(std::make_pair(HeapIterator(item_vec, tranc_id),
                                          HeapIterator()));
}
} // namespace my_tiny_lsm