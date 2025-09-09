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
  spdlog::info("Skiplist created with max level {}", max_level);
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

// in skiplist.cpp
// ... (previous code) ...
void Skiplist::put(const std::string &key, const std::string &value,
                   uint64_t tranction_id) {

  std::vector<std::shared_ptr<SkiplistNode>> update(max_level, nullptr);
  auto current = head;

  // 使用一个临时的、仅用于比较的节点，确保多版本排序正确
  auto temp_node_for_compare = std::make_shared<SkiplistNode>(key, value, tranction_id, 1);

  // 1. 从当前有效最高层开始，查找每一层的前驱节点
  for (int i = current_level - 1; i >= 0; i--) {
    while (current->forward_[i] && *current->forward_[i] < *temp_node_for_compare) {
      current = current->forward_[i];
    }
    update[i] = current;
  }

  // 2. 生成新节点的随机层高
  int new_node_level = random_level();

  // 3. 如果新节点的层高超过当前跳表的最大层高，才更新 current_level 和 update 数组
  if (new_node_level > current_level) {
    for (int i = current_level; i < new_node_level; i++) {
      update[i] = head;
    }
    // 只有在这种情况下才更新 current_level
    current_level = new_node_level;
  }

  // 4. 创建并链接新节点
  auto new_node =
      std::make_shared<SkiplistNode>(key, value, tranction_id, new_node_level);

  for (int i = 0; i < new_node_level; i++) {
    // 设置新节点的前向指针
    new_node->forward_[i] = update[i]->forward_[i];
    
    // 更新后继节点的后向指针
    if (new_node->forward_[i]) {
      new_node->forward_[i]->set_backward(i, new_node);
    }
    
    // 更新前驱节点的前向指针
    update[i]->forward_[i] = new_node;
    // 设置新节点的后向指针
    new_node->set_backward(i, update[i]);
  }

  // 5. 更新跳表的总大小
  size_bytes += key.size() + value.size() + sizeof(uint64_t);
}
// ... (rest of the file) ...
// void Skiplist::put(const std::string &key, const std::string &value,
//                    uint64_t tranction_id) {

//   std::vector<std::shared_ptr<SkiplistNode>> update(max_level, nullptr);

//   int new_level = std::max(random_level(), current_level);
//   auto new_node =
//       std::make_shared<SkiplistNode>(key, value, tranction_id, new_level);

//   auto current = head;
//   for (int i = current_level - 1; i >= 0; i--) {
//     while (current->forward_[i] && *current->forward_[i] < *new_node) {
//       current = current->forward_[i];
//     }
//     // now current is the largest node less than new_node at level i
//     update[i] = current;
//   }
//   current = current->forward_[0];
//   //   if the key already exists, update the value and transaction_id
//   if (current && current->key_ == key &&
//       current->tranction_id_ == tranction_id) {
//     size_bytes += value.size() - current->value_.size();
//     current->value_ = value;
//     current->tranction_id_ = tranction_id;
//     spdlog::info("Updated key: {}, transaction_id: {} with new value", key,
//                  tranction_id);
//     return;
//   }
//   // need update
//   if (new_level > current_level) {
//     for (int i = current_level; i < new_level; i++) {
//       update[i] = head;
//     }
//   }
//   int random_bits = dis_level(gen);
//   size_bytes += key.size() + value.size() + sizeof(uint64_t);
//   // for (int i = 0; i < new_level; i++) {
//   //   bool need_update = false;
//   //   if (i == 0 || (new_level > current_level) || (random_bits & (1 << i))) {
//   //     need_update = true;
//   //   }
//   //   if (need_update) {
//   //     new_node->forward_[i] = update[i]->forward_[i];
//   //     if (new_node->forward_[i]) {
//   //       new_node->forward_[i]->set_backward(i, new_node);
//   //     }
//   //     update[i]->forward_[i] = new_node;
//   //     new_node->set_backward(i, update[i]);
//   //   } else {
//   //     spdlog::info("Skip updating level {} for key {}", i, key);
//   //     break;
//   //   }
//   // }
//   for(int i=0;i<new_level;i++){
//     new_node->forward_[i] = update[i]->forward_[i];
//     if (new_node->forward_[i]) {
//       new_node->forward_[i]->set_backward(i, new_node);
//     }
//     update[i]->forward_[i] = new_node;
//     new_node->set_backward(i, update[i]);
//   }
//   current_level = new_level;
// }

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
// in skiplist.cpp
void Skiplist::remove(const std::string &key) {
  std::vector<std::shared_ptr<SkiplistNode>> update(max_level, nullptr);
  auto current = head;

  // 1. 循环删除，确保所有匹配 key 的版本都被处理
  while (true) {
    // 每次循环都重新查找前驱节点，因为上一次删除可能改变了结构
    current = head;
    
    // 2. 从当前的有效最高层开始查找 (修正了循环边界)
    for (int i = current_level - 1; i >= 0; --i) {
      while (current->forward_[i] && current->forward_[i]->key_ < key) {
        current = current->forward_[i];
      }
      update[i] = current;
    }

    // 移动到最底层，找到第一个可能匹配的节点
    auto node_to_delete = current->forward_[0];

    // 如果下一个节点不是我们要找的key，说明所有该key的版本都已删除，退出循环
    if (!node_to_delete || node_to_delete->key_ != key) {
      break; 
    }

    // --- 执行删除操作 ---

    // 3. 更新每一层的 forward 指针，跳过目标节点 (循环更清晰)
    for (int i = 0; i < node_to_delete->forward_.size(); ++i) {
      // 确保前驱节点的下一个节点确实是我们要删除的节点
      if (update[i]->forward_[i] == node_to_delete) {
        update[i]->forward_[i] = node_to_delete->forward_[i];
      }
    }

    // 更新 backward 指针
    for (int i = 0; i < node_to_delete->backward_.size(); ++i) {
      if (node_to_delete->forward_[i]) {
        node_to_delete->forward_[i]->set_backward(i, update[i]);
      }
    }

    // 4. 正确更新跳表的内存大小
    size_bytes -= (node_to_delete->key_.size() + node_to_delete->value_.size() + sizeof(uint64_t));
  }

  // 5. 在所有删除操作完成后，统一更新跳表的当前层级
  while (current_level > 1 && head->forward_[current_level - 1] == nullptr) {
    current_level--;
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
std::optional<std::pair<SkiplistIterator, SkiplistIterator>>
Skiplist::iters_monotony_predicate(
    std::function<int(const std::string &)> predicate) {
  
  auto current = head;

  // 1. 粗略定位：从高层向低层快速逼近目标区域的某个节点
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

  // 它的下一个节点 current->forward_[0] 是第一个可能满足条件的节点
  current = current->forward_[0];

  // 如果下一个节点不存在，或者它的 key 不满足条件，则说明没有匹配项
  if (!current || predicate(current->key_) != 0) {
    return std::nullopt;
  }
  
  // 2. 精确查找范围的起始点 (begin_it)
  // 我们需要从 current 出发，向后查找第一个满足条件的节点
  auto begin_node = current;
  for (int i = current_level - 1; i >= 0; i--) {
    while (true) {
      auto backward = begin_node->backward_[i].lock();
      // 停止条件是前一个节点不存在、是头节点、或不满足谓词
      if (!backward || backward == head || predicate(backward->key_) != 0) {
        break;
      }
      // 否则，继续后退
      begin_node = backward;
    }
  }
  SkiplistIterator begin_it(begin_node);

  // 3. 精确查找范围的结束点 (end_it)
  // 从找到的第一个满足条件的节点 begin_node 开始，向前查找最后一个满足条件的节点
  auto end_node = begin_node;
  for (int i = current_level - 1; i >= 0; i--) {
    while (true) {
      auto forward = end_node->forward_[i];
      // 停止条件是后一个节点不存在，或者不满足谓词
      if (!forward || predicate(forward->key_) != 0) {
        break;
      }
      // 否则，继续前进
      end_node = forward;
    }
  }
  
  // end_it 应该是最后一个满足条件的节点的下一个节点
  SkiplistIterator end_it(end_node->forward_[0]);

  return std::make_optional<std::pair<SkiplistIterator, SkiplistIterator>>(
      begin_it, end_it);
}
// std::optional<std::pair<SkiplistIterator, SkiplistIterator>>
// Skiplist::iters_monotony_predicate(
//     std::function<int(const std::string &)> predicate) {
  
//   auto current = head;
//   spdlog::trace("Predicate search: Starting coarse search.");
//   // 1. 粗略定位：从高层向低层快速逼近目标区域的某个节点
//   for (int i = current_level - 1; i >= 0; i--) {
//     while (true) {
//       auto forward = current->forward_[i];
//       if (!forward) {
//         break; // 到达本层末尾，下降一层
//       }
//       int direction = predicate(forward->key_);
//       if (direction > 0) {
//         // key太小，需要向右移动
//         current = forward;
//       } else {
//         // key满足条件(==0)或太大(<0)，不再向右移动，下降一层
//         break;
//       }
//     }
//   }
//   spdlog::trace("Predicate search: Coarse search completed.");
//   // 它的下一个节点 current->forward_[0] 是第一个可能满足条件的节点
//   current = current->forward_[0];

//   // 如果下一个节点不存在，或者它的 key 不满足条件，则说明没有匹配项
//   if (!current || predicate(current->key_) != 0) {
//     spdlog::trace("Predicate search: Coarse search found no matching node.");
//     return std::nullopt;
//   }
  
//   spdlog::trace("Predicate search: Coarse search found a candidate node with key '{}'", current->key_);

//   // 2. 精确查找范围的起始点 (begin_it)
//   // 我们需要从 current 出发，向后查找第一个满足条件的节点
//   auto begin_node = current;
//   for (int i = current_level - 1; i >= 0; i--) {
//     while (true) {
//       auto backward = begin_node->backward_[i].lock();
//       // 【修正一】: 停止条件是前一个节点不满足谓词 (predicate != 0)
//       if (!backward || backward == head || predicate(backward->key_) != 0) {
//         break;
//       }
//       // 否则，继续后退
//       begin_node = backward;
//     }
//   }
//   SkiplistIterator begin_it(begin_node);
//   spdlog::trace("Predicate search: Found final begin_node with key '{}'", begin_it.get_key());

//   // 3. 精确查找范围的结束点 (end_it)
//   // 从找到的第一个满足条件的节点 begin_node 开始，向前查找最后一个满足条件的节点
//   auto end_node = begin_node;
//   for (int i = current_level - 1; i >= 0; i--) {
//     while (true) {
//       auto forward = end_node->forward_[i];
//       if (!forward || predicate(forward->key_) != 0) {
//         break;
//       }
//       // 否则，继续前进
//       end_node = forward;
//     }
//   }
  
//   // end_it 应该是最后一个满足条件的节点的下一个节点
//   SkiplistIterator end_it(end_node->forward_[0]);
//   if (!end_it.is_end()) {
//     spdlog::trace("Predicate search: Found final end_node with key '{}', end_iterator points to '{}'", end_node->key_, end_it.get_key());
//   } else {
//     spdlog::trace("Predicate search: Found final end_node with key '{}', end_iterator points to end()", end_node->key_);
//   }


//   return std::make_optional<std::pair<SkiplistIterator, SkiplistIterator>>(
//       begin_it, end_it);
// }

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