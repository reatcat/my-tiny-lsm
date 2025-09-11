#include "iterator/iterator.h"
#include "../../include/iterator/iterator.h"
#include <tuple>
#include <vector>

namespace my_tiny_lsm {

// *************************** SearchItem ***************************
bool operator<(const SearchItem &a, const SearchItem &b) {
  if (a.key_ != b.key_) {
    return a.key_ < b.key_;
  }
  if (a.transaction_id_ > b.transaction_id_) {
    return true;
  }
  if (a.level_ < b.level_) {
    return true;
  }
  return a.idx_ < b.idx_;
}

bool operator>(const SearchItem &a, const SearchItem &b) {
  if (a.key_ != b.key_) {
    return a.key_ > b.key_;
  }
  if (a.transaction_id_ < b.transaction_id_) {
    return true;
  }
  if (a.level_ < b.level_) {
    return true;
  }
  return a.idx_ > b.idx_;
}

bool operator==(const SearchItem &a, const SearchItem &b) {
  return a.idx_ == b.idx_ && a.key_ == b.key_;
}

// *************************** HeapIterator ***************************
HeapIterator::HeapIterator(bool skip_deleted) : skip_deleted_(skip_deleted) {
  // 默认构造函数
}
HeapIterator::HeapIterator(std::vector<SearchItem> item_vec,
                           uint64_t max_transaction_id, bool skip_deleted)
    : skip_deleted_(skip_deleted), max_transaction_id_(max_transaction_id) {
  for (auto &item : item_vec) {
    items.push(std::move(item));
  }
  while (!top_value_legal()) {
    skip_by_transaction_id();

    while (!items.empty() && items.top().value_.empty()) {
      auto del_key = items.top().key_;
      while (!items.empty() && items.top().key_ == del_key) {
        items.pop();
      }
    }
  }
}

HeapIterator::pointer HeapIterator::operator->() const {
  update_current();
  return current_.get();
}

HeapIterator::value_type HeapIterator::operator*() const {
  return std::make_pair(items.top().key_, items.top().value_);
}

BaseIterator &HeapIterator::operator++() {
  if (items.empty()) {
    return *this;
  }
  auto old_item = items.top();
  items.pop();

  while (!items.empty() && items.top().key_ == old_item.key_) {
    items.pop();
  }
  while (!top_value_legal()) {
    skip_by_transaction_id();

    while (!items.empty() && items.top().value_.empty()) {
      // 如果value为空，value_表明懒删除
      auto del_key = items.top().key_;
      while (!items.empty() && items.top().key_ == del_key) {
        items.pop();
      }
    }
  }
  return *this;
}
BaseIterator &HeapIterator::operator--() {
  // 不支持向后迭代
  return *this;
}


bool HeapIterator::operator==(const BaseIterator &other) const {
  if (other.type() != IteratorType::HeapIterator) {
    return false;
  }
  // 多态的安全转型
  auto other_heap = dynamic_cast<const HeapIterator &>(other);
  if (items.empty() && other_heap.items.empty()) {
    return true;
  }
  if (items.empty() || other_heap.items.empty()) {
    return false;
  }
  return items.top() == other_heap.items.top();
}

bool HeapIterator::operator!=(const BaseIterator &other) const {
  return !(*this == other);
}

bool HeapIterator::top_value_legal() const {
  if (items.empty()) {
    return true;
  }
  if (max_transaction_id_ == 0) {
    return items.top().value_.size() > 0;
  }
  if (items.top().transaction_id_ <= max_transaction_id_) {
    if (skip_deleted_) {
      // 判断是否为空
      return items.top().value_.size() > 0;
    } else {
      return true;
    }
  }
  return false;
}
void HeapIterator::skip_by_transaction_id() {
  if (items.empty()) {
    return;
  }
  if (max_transaction_id_ == 0) {
    return;
  }
  while (!items.empty() && items.top().transaction_id_ > max_transaction_id_) {
    items.pop();
  }
}

bool HeapIterator::is_end() const { return items.empty(); }
bool HeapIterator::is_valid() const { return !items.empty(); }

void HeapIterator::update_current() const {
  if (!items.empty()) {
    current_ =
        std::make_shared<value_type>(items.top().key_, items.top().value_);

  } else {
    current_.reset();
  }
}

IteratorType HeapIterator::type() const {
  return IteratorType::HeapIterator;
}
uint64_t HeapIterator::get_transaction_id() const {
  return max_transaction_id_;
}
} // namespace my_tiny_lsm