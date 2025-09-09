#include "../iterator/iterator.h"
#include <cstddef>
#include <cstdint>
#include <functional>
#include <memory>
#include <mutex>
#include <optional>
#include <random>
#include <shared_mutex>
#include <string>
#include <sys/types.h>
#include <tuple>
#include <utility>
#include <vector>
namespace my_tiny_lsm {

struct SkiplistNode {
  std::string key_;
  std::string value_;
  uint64_t tranction_id_;
  std::vector<std::shared_ptr<SkiplistNode>> forward_;
  //   weak_ptr 防止循环引用导致内存泄漏
  std::vector<std::weak_ptr<SkiplistNode>> backward_;
  SkiplistNode(const std::string &key, const std::string &value,
               uint64_t transaction_id, int level)
      : key_(key), value_(value), tranction_id_(transaction_id),
        forward_(level, nullptr),
        backward_(level, std::weak_ptr<SkiplistNode>()) {}

  void set_backward(int level, std::shared_ptr<SkiplistNode> node) {
    if (level >= 0 && level < backward_.size()) {
      backward_[level] = std::weak_ptr<SkiplistNode>(node);
    }
  }
  bool operator==(const SkiplistNode &other) const {
    return key_ == other.key_ && value_ == other.value_ &&
           tranction_id_ == other.tranction_id_;
  }

  bool operator!=(const SkiplistNode &other) const { return !(*this == other); }
  // 事务越大的排在前面
  bool operator<(const SkiplistNode &other) const {
    if (key_ == other.key_) {
      return tranction_id_ > other.tranction_id_;
    }
    return key_ < other.key_;
  }
  bool operator>(const SkiplistNode &other) const {
    if (key_ == other.key_) {
      return tranction_id_ < other.tranction_id_;
    }
    return key_ > other.key_;
  }
};

class SkiplistIterator : public BaseIterator {
public:
  SkiplistIterator(std::shared_ptr<SkiplistNode> node) : current(node){};
  SkiplistIterator() : current(nullptr), lock(nullptr){};
  // friend class Skiplist;
  virtual BaseIterator &operator++() override;
  virtual BaseIterator &operator--() override {
    if (current) {
      auto backward = current->backward_[0].lock();
      current = backward;
    }
    return *this;
  }
  virtual bool operator==(const BaseIterator &other) const override;
  virtual bool operator!=(const BaseIterator &other) const override;
  virtual value_type operator*() const override;
  virtual IteratorType type() const override;
  virtual bool is_end() const override;
  virtual bool is_valid() const override;
  std::string get_key() const;
  std::string get_value() const;
  uint64_t get_tranction_id() const override;

private:
  std::shared_ptr<SkiplistNode> current;
  std::shared_ptr<std::shared_lock<std::shared_mutex>> lock;
};

class Skiplist {
private:
  std::shared_ptr<SkiplistNode> head;
  int max_level;
  int current_level;
  size_t size_bytes;
  //   random engine for level generation
  std::uniform_int_distribution<> dis_01;
  std::uniform_int_distribution<> dis_level;
  std::mt19937 gen;

  int random_level();

public:
  Skiplist(int max_level = 16);
  ~Skiplist() {
    auto current = head;
    while (current && current->forward_[0]) {
      auto next = current->forward_[0];
      for (int i = 0; i < current->forward_.size(); ++i) {
        current->forward_[i].reset();
      }
      current = next;
    }
    head.reset();
  }

  // 插入或更新键值对
  // 这里不对 tranction_id 进行检查，由上层保证 tranction_id 的合法性
  void put(const std::string &key, const std::string &value,
           uint64_t tranction_id);

  // 查找键对应的值
  // 事务 id 为0 表示没有开启事务
  // 否则只能查找事务 id 小于等于 tranction_id 的值
  // 返回值: 如果找到，返回 value 和 tranction_id，否则返回空
  SkiplistIterator get(const std::string &key, uint64_t tranction_id);

  // !!! 这里的 remove 是跳表本身真实的 remove,  lsm 应该使用 put 空值表示删除
  void remove(const std::string &key); // 删除键值对

  // 将跳表数据刷出，返回有序键值对列表
  // value 为 真实 value 和 tranction_id 的二元组
  std::vector<std::tuple<std::string, std::string, uint64_t>> flush();

  size_t get_size();

  void clear(); // 清空跳表，释放内存

  SkiplistIterator begin();
  SkiplistIterator begin_preffix(const std::string &preffix);

  SkiplistIterator end();
  SkiplistIterator end_preffix(const std::string &preffix);

  std::optional<std::pair<SkiplistIterator, SkiplistIterator>>
  iters_monotony_predicate(std::function<int(const std::string &)> predicate);

  void print_skiplist();
};
} // namespace my_tiny_lsm