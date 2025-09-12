#include "../../include/sst/sst_iterator.h"
#include "../../include/sst/sst.h"
#include <cstddef>
#include <optional>
#include <stdexcept>

namespace my_tiny_lsm {

// predicate返回值:
//   0: 谓词
//   >0: 不满足谓词, 需要向右移动
//   <0: 不满足谓词, 需要向左移动
std::optional<std::pair<SSTableIterator, SSTableIterator>>
sst_iters_monotony_predicate(
    std::shared_ptr<SST> sst, uint64_t tranc_id,
    std::function<int(const std::string &)> predicate) {
  std::optional<SSTableIterator> final_begin = std::nullopt;
  std::optional<SSTableIterator> final_end = std::nullopt;
  for (int block_idx = 0; block_idx < sst->meta_entries.size(); block_idx++) {
    auto block = sst->read_block(block_idx);

    BlockMeta &meta_i = sst->meta_entries[block_idx];
    if (predicate(meta_i.first_key) < 0 || predicate(meta_i.last_key) > 0) {
      break;
    }

    auto result_i = block->get_monotony_predicate_iters(tranc_id, predicate);
    if (result_i.has_value()) {
      auto [i_begin, i_end] = result_i.value();
      if (!final_begin.has_value()) {
        auto tmp_it = SSTableIterator(sst, tranc_id);
        tmp_it.set_block_idx(block_idx);
        tmp_it.set_block_it(i_begin);
        final_begin = tmp_it;
      }
      auto tmp_it = SSTableIterator(sst, tranc_id);
      tmp_it.set_block_idx(block_idx);
      tmp_it.set_block_it(i_end);
      if (tmp_it.is_end() && tmp_it.m_block_idx == sst->num_blocks()) {
        tmp_it.set_block_it(nullptr);
      }
      final_end = tmp_it;
    }
  }
  if (!final_begin.has_value() || !final_end.has_value()) {
    return std::nullopt;
  }
  return std::make_pair(final_begin.value(), final_end.value());
}

SSTableIterator::SSTableIterator(std::shared_ptr<SST> sst, uint64_t tranc_id)
    : m_sst(sst), m_block_idx(0), m_block_it(nullptr), max_tranc_id_(tranc_id) {
  if (m_sst) {
    seek_first();
  }
}

SSTableIterator::SSTableIterator(std::shared_ptr<SST> sst,
                                 const std::string &key, uint64_t tranc_id)
    : m_sst(sst), m_block_idx(0), m_block_it(nullptr), max_tranc_id_(tranc_id) {
  if (m_sst) {
    seek(key);
  }
}

void SSTableIterator::set_block_idx(size_t idx) { m_block_idx = idx; }
void SSTableIterator::set_block_it(std::shared_ptr<BlockIterator> it) {
  m_block_it = it;
}

void SSTableIterator::seek_first() {
  if (!m_sst || m_sst->num_blocks() == 0) {
    m_block_it = nullptr;
    return;
  }

  m_block_idx = 0;
  auto block = m_sst->read_block(m_block_idx);
  m_block_it = std::make_shared<BlockIterator>(block, 0, max_tranc_id_);
}

void SSTableIterator::seek(const std::string &key) {
  if (!m_sst) {
    m_block_it = nullptr;
    return;
  }

  try {
    m_block_idx = m_sst->find_block_idx(key);
    if (m_block_idx == -1 || m_block_idx >= m_sst->num_blocks()) {
      // 置为 end
      // TODO: 这个边界情况需要添加单元测试
      m_block_it = nullptr;
      m_block_idx = m_sst->num_blocks();
      return;
    }
    auto block = m_sst->read_block(m_block_idx);
    if (!block) {
      m_block_it = nullptr;
      return;
    }
    m_block_it = std::make_shared<BlockIterator>(block, key, max_tranc_id_);
    if (m_block_it->is_end()) {
      // block 中找不到
      m_block_idx = m_sst->num_blocks();
      m_block_it = nullptr;
      return;
    }
  } catch (const std::exception &) {
    m_block_it = nullptr;
    return;
  }
}

std::string SSTableIterator::key() {
  if (!m_block_it) {
    throw std::runtime_error("Iterator is invalid");
  }
  return (*m_block_it)->first;
}

std::string SSTableIterator::value() {
  if (!m_block_it) {
    throw std::runtime_error("Iterator is invalid");
  }
  return (*m_block_it)->second;
}

BaseIterator &SSTableIterator::operator++() {
  if (!m_block_it) { // 添加空指针检查
    return *this;
  }
  ++(*m_block_it);
  if (m_block_it->is_end()) {
    m_block_idx++;
    if (m_block_idx < m_sst->num_blocks()) {
      // 读取下一个block
      auto next_block = m_sst->read_block(m_block_idx);
      BlockIterator new_blk_it(next_block, 0, max_tranc_id_);
      (*m_block_it) = new_blk_it;
    } else {
      // 没有下一个block
      m_block_it = nullptr;
    }
  }
  return *this;
}

bool SSTableIterator::operator==(const BaseIterator &other) const {
  if (other.type() != IteratorType::SSTableIterator) {
    return false;
  }
  auto other2 = dynamic_cast<const SSTableIterator &>(other);
  if (m_sst != other2.m_sst || m_block_idx != other2.m_block_idx) {
    return false;
  }

  if (!m_block_it && !other2.m_block_it) {
    return true;
  }

  if (!m_block_it || !other2.m_block_it) {
    return false;
  }

  return *m_block_it == *other2.m_block_it;
}

bool SSTableIterator::operator!=(const BaseIterator &other) const {
  return !(*this == other);
}

SSTableIterator::value_type SSTableIterator::operator*() const {
  if (!m_block_it) {
    throw std::runtime_error("Iterator is invalid");
  }
  return (**m_block_it);
}

IteratorType SSTableIterator::type() const {
  return IteratorType::SSTableIterator;
}

uint64_t SSTableIterator::get_transaction_id() const { return max_tranc_id_; }
bool SSTableIterator::is_end() const { return !m_block_it; }

bool SSTableIterator::is_valid() const {
  return m_block_it && !m_block_it->is_end() &&
         m_block_idx < m_sst->num_blocks();
}
SSTableIterator::pointer SSTableIterator::operator->() const {
  update_current();
  return &(*cached_value);
}

void SSTableIterator::update_current() const {
  if (!cached_value && m_block_it && !m_block_it->is_end()) {
    cached_value = *(*m_block_it);
  }
}

std::pair<HeapIterator, HeapIterator>
SSTableIterator::merge_sst_iterator(std::vector<SSTableIterator> iter_vec,
                                    uint64_t tranc_id) {
  if (iter_vec.empty()) {
    return std::make_pair(HeapIterator(), HeapIterator());
  }

  HeapIterator it_begin(false); // 不跳过删除元素
  for (auto &iter : iter_vec) {
    while (iter.is_valid() && !iter.is_end()) {
      it_begin.items.emplace(
          iter.key(), iter.value(), -iter.m_sst->get_sst_id(), 0,
          tranc_id); // ! 此处的level暂时没有作用, 都作用于同一层的比较
      ++iter;
    }
  }
  return std::make_pair(it_begin, HeapIterator());
}
} // namespace my_tiny_lsm