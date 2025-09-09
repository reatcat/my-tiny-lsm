#include "../../include/skiplist/skiplist.h"
#include <cstdint>
#include <iostream>
#include <spdlog/spdlog.h>
#include <stdexcept>
#include <tuple>
#include <utility>

namespace my_tiny_lsm {

BaseIterator &SkiplistIterator::operator++() {
  if (current) {
    current = current->forward_[0];
  }
  return *this;
}

bool SkiplistIterator::operator==(const BaseIterator &other) const {
  if (auto *other_skiplist = dynamic_cast<const SkiplistIterator *>(&other)) {
    return current == other_skiplist->current;
  }
  return false;
}

bool SkiplistIterator::operator!=(const BaseIterator &other) const {
  return !(*this == other);
}

SkiplistIterator::value_type SkiplistIterator::operator*() const {
  if (!current) {
    throw std::runtime_error("Dereferencing end iterator");
  }
  return {current->key_, current->value_};
}

IteratorType SkiplistIterator::type() const {
  return IteratorType::SkipListIterator;
}

bool SkiplistIterator::is_valid() const {
  return current && !current->key_.empty();
}
bool SkiplistIterator::is_end() const { return current == nullptr; }

std::string SkiplistIterator::get_key() const { return current->key_; }
std::string SkiplistIterator::get_value() const { return current->value_; }
uint64_t SkiplistIterator::get_tranction_id() const {
  return current->tranction_id_;
}

Skiplist::Skiplist(int max_level) : max_level(max_level), current_level(1) {
  head = std::make_shared<SkiplistNode>("", "", 0, max_level);
  dis_01 = std::uniform_int_distribution<>(0, 1);
  dis_level = std::uniform_int_distribution<>(0, (1 << max_level) - 1);
  gen = std::mt19937(std::random_device()());
}

int Skiplist::random_level() {
  int level = 1;
  // 通过"抛硬币"的方式随机生成层数：
  // - 每次有50%的概率增加一层
  // - 确保层数分布为：第1层100%，第2层50%，第3层25%，以此类推
  // - 层数范围限制在[1, max_level]之间，避免浪费内存
  while (dis_01(gen) && level < max_level) {
    level++;
  }
  return level;
}
void Skiplist::put(const std::string &key, const std::string &value,
                   uint64_t tranction_id) {

  std::vector<std::shared_ptr<SkiplistNode>> update(max_level, nullptr);

  int new_level = std::max(random_level(), current_level);
  auto new_node =
      std::make_shared<SkiplistNode>(key, value, tranction_id, new_level);

  auto current = head;
  for (int i = current_level - 1; i >= 0; i--) {
    while (current->forward_[i] && *current->forward_[i] < *new_node) {
      current = current->forward_[i];
    }
    // now current is the largest node less than new_node at level i
    update[i] = current;
  }
  current = current->forward_[0];
  //   if the key already exists, update the value and transaction_id
  if (current && current->key_ == key &&
      current->tranction_id_ == tranction_id) {
    size_bytes += value.size() - current->value_.size();
    current->value_ = value;
    current->tranction_id_ = tranction_id;
    return;
  }
  // need update
  if (new_level > current_level) {
    for (int i = current_level; i < new_level; i++) {
      update[i] = head;
    }
  }
  int random_bits = dis_level(gen);
  size_bytes += key.size() + value.size() + sizeof(uint64_t);
  for (int i = 0; i < new_level; i++) {
    bool need_update = false;
    if (i == 0 || (new_level > current_level) || (random_bits & (1 << i))) {
      need_update = 1;
    }
    if (need_update) {
      new_node->forward_[i] = update[i]->forward_[i];
      if (new_node->forward_[i]) {
        new_node->forward_[i]->set_backward(i, new_node);
      }
      update[i]->forward_[i] = new_node;
      new_node->set_backward(i, update[i]);
    } else {
      break;
    }
  }
  current_level = new_level;
}

SkiplistIterator Skiplist::get(const std::string &key, uint64_t tranction_id) {
  auto current = head;
  for (int i = current_level - 1; i >= 0; i--) {
    while (current->forward_[i] && current->forward_[i]->key_ < key) {
      current = current->forward_[i];
    }
  }
  current = current->forward_[0];
  if (tranction_id == 0) {
    if (current && current->key_ == key) {
      // return SkiplistIterator{current};
      return SkiplistIterator(current);
    }
  } else {
    while (current && current->key_ == key) {
      if (current->tranction_id_ <= tranction_id) {
        return SkiplistIterator(current);
      }
      // current transaction_id is greater than the given tranction_id, keep
      // looking
      current = current->forward_[0];
    }
  }
  return SkiplistIterator{nullptr};
}

// lsm-tree use lazy deletion don't use this function
void Skiplist::remove(const std::string &key) {
  // Find the node to remove
  auto current = head;
  std::vector<std::shared_ptr<SkiplistNode>> update(max_level, nullptr);
  for (int i = current_level - 1; i >= 0; i--) {
    while (current->forward_[i] && current->forward_[i]->key_ < key) {
      current = current->forward_[i];
    }
    update[i] = current;
  }
  current = current->forward_[0];
  // If the node is found, mark it as deleted
  if (current && current->key_ == key) {
    for (int i = 0; i < current->forward_.size(); i++) {
      if (update[i]) {
        update[i]->forward_[i] = current->forward_[i];
      }
    }
    // Update the size
    size_bytes -=
        current->key_.size() + current->value_.size() + sizeof(uint64_t);
  }
}

std::vector<std::tuple<std::string, std::string, uint64_t>> Skiplist::flush() {
  std::vector<std::tuple<std::string, std::string, uint64_t>> result;
  auto node = head->forward_[0];
  while (node) {
    result.emplace_back(
        std::make_tuple(node->key_, node->value_, node->tranction_id_));
    node = node->forward_[0];
  }
  return result;
}

size_t Skiplist::get_size() { return size_bytes; }

void Skiplist::clear() {
  head = std::make_shared<SkiplistNode>("", "", 0, max_level);
  size_bytes = 0;
}

SkiplistIterator Skiplist::begin() {
  // return SkiplistIterator{head->forward_[0]};
  return SkiplistIterator(head->forward_[0]);
}

SkiplistIterator Skiplist::end() { return SkiplistIterator{nullptr}; }

SkiplistIterator Skiplist::begin_preffix(const std::string &preffix) {
  auto current = head;
  for (int i = current_level - 1; i >= 0; i--) {
    while (current->forward_[i] && current->forward_[i]->key_ < preffix) {
      current = current->forward_[i];
    }
  }
  current = current->forward_[0];
  return SkiplistIterator(current);
}
SkiplistIterator Skiplist::end_preffix(const std::string &preffix) {
  auto current = head;
  for (int i = current_level - 1; i >= 0; i--) {
    while (current->forward_[i] && current->forward_[i]->key_ < preffix) {
      current = current->forward_[i];
    }
  }
  current = current->forward_[0];
  while (current && current->key_.substr(0, preffix.size()) == preffix) {
    current = current->forward_[0];
  }
  return SkiplistIterator(current);
}

// 返回第一个满足谓词的位置和最后一个满足谓词的迭代器
// 如果不存在, 范围nullptr
// 谓词作用于key, 且保证满足谓词的结果只在一段连续的区间内, 例如前缀匹配的谓词
// predicate返回值:
//   0: 谓词
//   >0: 不满足谓词, 需要向右移动
//   <0: 不满足谓词, 需要向左移动
// ...existing code...
std::optional<std::pair<SkiplistIterator, SkiplistIterator>>
Skiplist::iters_monotony_predicate(
    std::function<int(const std::string &)> predicate) {
  auto current = head;

  // 1. 粗略定位：从高层向低层快速逼近目标区域的某个节点
  // 目标是找到任意一个 predicate(key) == 0 的节点
  for (int i = current_level - 1; i >= 0; i--) {
    while (true) {
      auto forward = current->forward_[i];
      if (!forward) {
        break; // 到达本层末尾，下降一层
      }
      int direction = predicate(forward->key_);
      if (direction > 0) {
        // key太小，需要向右移动
        current = forward;
      } else {
        // key满足条件(==0)或太大(<0)，不再向右移动，下降一层
        break;
      }
    }
  }

  // 此时 current 是最后一个 predicate(key) > 0 的节点
  // 或者 current 是 head (如果所有 key 都 >= 目标)
  // 它的下一个节点 current->forward_[0] 是第一个可能满足条件的节点
  current = current->forward_[0];

  // 如果下一个节点不存在，或者它的 key 不满足条件，则说明没有匹配项
  if (!current || predicate(current->key_) != 0) {
    return std::nullopt;
  }

  // 2. 精确查找范围的起始点 (begin_it)
  // 此时 current 已经是一个满足条件的节点，但未必是第一个
  // 我们需要从 current 出发，向后查找第一个满足条件的节点
  auto begin_node = current;
  for (int i = current_level - 1; i >= 0; i--) {
    while (true) {
      auto backward = begin_node->backward_[i].lock();
      if (!backward || predicate(backward->key_) != 0 || backward == head) {
        break; // 到达本层头部或不满足条件，停止后退
      }
      begin_node = backward; // 继续后退
    }
  }
  // 闭区间
  SkiplistIterator begin_it(begin_node);
  auto end_node = begin_node;
  for (int i = current_level - 1; i >= 0; i--) {
    while (true) {
      auto forward = end_node->forward_[i];
      if (!forward || predicate(forward->key_) != 0) {
        break; // 到达本层末尾或不满足条件，停止前进
      }
      end_node = forward; // 继续前进
    }
  }
  // end_it 应该是最后一个满足条件的节点的下一个节点
  SkiplistIterator end_it(end_node->forward_[0]);

  return std::make_pair(begin_it, end_it);
}

void Skiplist::print_skiplist() {
  for (int i = 0; i < max_level; i++) {
    std::cout << "level " << i << ": ";
    auto current = head->forward_[i];
    while (current) {
      std::cout << current->key_ << " ";
      current = current->forward_[i];
      if (current) {
        std::cout << "-> ";
      }
    }
    std::cout << std::endl;
  }
  std::cout << "Total size (bytes): " << size_bytes << std::endl;
}
} // namespace my_tiny_lsm