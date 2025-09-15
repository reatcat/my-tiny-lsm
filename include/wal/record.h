#pragma once

#include <cstdint>
#include <iostream>
#include <stdexcept>
#include <string>
#include <vector>

namespace my_tiny_lsm {
enum class OperationType {
  CREATE,
  COMMIT,
  ROLLBACK,
  PUT,
  DELETE,
};

class Record {
private:
  Record() = default;

public:
  static Record createRecord(uint64_t tranc_id);
  static Record commitRecord(uint64_t tranc_id);
  static Record rollbackRecord(uint64_t tranc_id);
  static Record putRecord(uint64_t tranc_id, const std::string &key,
                          const std::string &value);
  static Record deleteRecord(uint64_t tranc_id, const std::string &key);

  std::vector<uint8_t> encode() const;
  static std::vector<Record> decode(const std::vector<uint8_t> &data);
  uint64_t getTrancId() const { return tranc_id_; };
  OperationType getOpType() const { return operation_type_; };
  std::string getKey() const { return key_; };
  std::string getValue() const { return value_; };
  uint16_t getRecordSize() const { return record_len_; };
  bool operator==(const Record &other) const;
  bool operator!=(const Record &other) const;

private:
  uint64_t tranc_id_;
  OperationType operation_type_;
  std::string key_;
  std::string value_;
  uint16_t record_len_;
};
} // namespace my_tiny_lsm