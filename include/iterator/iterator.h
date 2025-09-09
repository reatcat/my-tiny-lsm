#pragma once

#include <cstdint>
#include <memory>
#include <optional>
#include <queue>
#include <string>
#include <utility>

namespace my_tiny_lsm {

enum class IteratorType {
  SkipListIterator,
  MemTableIterator,
  SSTableIterator,
  HeapIterator,
  TwoMergeIterator,
  ConcactIterator,
  LevelIterator,
};
class BaseIterator {
public:
  // using alias for value_type
  using value_type = std::pair<std::string, std::optional<std::string>>;
  using pointer = value_type *;
  using reference = value_type &;
  // =0 makes the function pure virtual need to be implemented by derived
  // classes
  virtual BaseIterator &operator++() = 0;
  virtual BaseIterator &operator--() = 0;
  virtual bool operator==(const BaseIterator &other) const = 0;
  virtual bool operator!=(const BaseIterator &other) const = 0;
  virtual value_type operator*() const = 0;
  virtual IteratorType type() const = 0;
  virtual uint64_t get_tranction_id() const = 0;
  virtual bool is_end() const = 0;
  virtual bool is_valid() const = 0;
  // virtual destructor for base class
  virtual ~BaseIterator() = default;
};
class SSTableIterator;

struct SearchItem {
  std::string key_;
  std::string value_;
  uint64_t tranction_id_;
  int idx_;
  int level_;

  SearchItem() = default;
  SearchItem(std::string k, std::string v, int i, int l, uint64_t tranction_id)
      : key_(std::move(k)), value_(std::move(v)), idx_(i), level_(l),
        tranction_id_(tranction_id) {}
};

bool operator>(const SearchItem &a, const SearchItem &b);
bool operator<(const SearchItem &a, const SearchItem &b);
bool operator==(const SearchItem &a, const SearchItem &b);
bool operator!=(const SearchItem &a, const SearchItem &b);

class HeapIterator : public BaseIterator {
  friend class SSTableIterator;

public:
  HeapIterator(bool skip_deleted = true);
  HeapIterator(std::vector<SearchItem> item_vec, uint64_t max_tranction_id,
               bool skip_deleted = true);

  pointer operator->() const;
  virtual value_type operator*() const override;
  BaseIterator &operator++() override;
  BaseIterator &operator--() override;
  BaseIterator &operator--(int) = delete;
  BaseIterator &operator++(int) = delete;
  virtual bool operator==(const BaseIterator &other) const override;
  virtual bool operator!=(const BaseIterator &other) const override;

  virtual IteratorType type() const override;
  virtual uint64_t get_tranction_id() const override;
  virtual bool is_end() const override;
  virtual bool is_valid() const override;
  ~HeapIterator() override = default;

private:
  bool top_value_legal() const;
  // skip
  void skip_by_tranction_id();

  void update_current() const;

  std::priority_queue<SearchItem, std::vector<SearchItem>,
                      std::greater<SearchItem>>
      items;
  mutable std::shared_ptr<value_type> current_;
  uint64_t max_tranction_id_;
  bool skip_deleted_;
};

} // namespace my_tiny_lsm