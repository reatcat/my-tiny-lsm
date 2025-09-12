#pragma once

#include "sst.h"
#include "sst_iterator.h"
#include <memory>
#include <vector>
// service for concact iterator to iterate multiple sst files in L0
namespace my_tiny_lsm {

class ConcactIterator : public BaseIterator {
private:
  SSTableIterator cur_iter;
  size_t cur_idx;
  std::vector<std::shared_ptr<SST>> ssts;
  uint64_t max_tranc_id_;

public:
  ConcactIterator(std::vector<std::shared_ptr<SST>> ssts,
                  uint64_t max_tranc_id);

  std::string key();
  std::string value();

  virtual value_type operator*() const override;
  virtual BaseIterator &operator++() override;
  virtual bool operator==(const BaseIterator &other) const override;
  virtual bool operator!=(const BaseIterator &other) const override;
  virtual IteratorType type() const override;
  virtual uint64_t get_transaction_id() const override;
  virtual bool is_end() const override;
  virtual bool is_valid() const override;
  pointer operator->() const;
}

} // namespace my_tiny_lsm