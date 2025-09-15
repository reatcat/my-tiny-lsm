#include "../../include/lsm/transaction.h"
#include "../../include/lsm/engine.h"
#include "../../include/utils/files.h"
#include "../../include/utils/set_operation.h"
#include <algorithm>
#include <cerrno>
#include <cstdint>
#include <cstring>
#include <filesystem>
#include <iostream>
#include <mutex>
#include <thread>

namespace my_tiny_lsm {
inline std::string isolation_level_to_string(const Isolationlevel &level) {
  switch (level) {
  case Isolationlevel::READ_UNCOMMITTED:
    return "READ_UNCOMMITTED";
  case Isolationlevel::READ_COMMITTED:
    return "READ_COMMITTED";
  case Isolationlevel::REPEATABLE_READ:
    return "REPEATABLE_READ";
  case Isolationlevel::SERIALIZABLE:
    return "SERIALIZABLE";
  default:
    return "UNKNOWN";
  }
}
TranContext::TranContext(uint64_t tranc_id, std::shared_ptr<LSMEngine> engine,
                         std::shared_ptr<TranManager> tranManager,
                         const enum Isolationlevel &isolation_level)
    : tranc_id_(tranc_id), engine_(std::move(engine)),
      tranManager_(std::move(tranManager)), isolation_level_(isolation_level) {
  operations.emplace_back(Record::createRecord(tranc_id_));
}

void TranContext::put(const std::string &key, const std::string &value) {
  auto isolation_level = get_isolation_level();

  operations.emplace_back(Record::putRecord(tranc_id_, key, value));

  if (isolation_level == Isolationlevel::READ_UNCOMMITTED) {
    auto pre_record = engine_->get(key, tranc_id_);
    rollback_map_[key] = pre_record;
    engine_->put(key, value, tranc_id_);
    return;
  }
  temp_map_[key] = value;
}

void TranContext::remove(const std::string &key) {
  auto isolation_level = get_isolation_level();

  operations.emplace_back(Record::deleteRecord(tranc_id_, key));

  if (isolation_level == Isolationlevel::READ_UNCOMMITTED) {
    auto pre_record = engine_->get(key, tranc_id_);
    rollback_map_[key] = pre_record;
    engine_->remove(key, tranc_id_);
    return;
  }
  temp_map_[key] = ""; // 标记删除
}

std::optional<std::string> TranContext::get(const std::string &key) {
  auto isolation_level = get_isolation_level();
  if (temp_map_.find(key) != temp_map_.end()) {
    return temp_map_[key];
  }
  std::optional<std::pair<std::string, uint64_t>> query;
  if (isolation_level == Isolationlevel::READ_UNCOMMITTED) {
    query = engine_->get(key, 0);
  } else if (isolation_level == Isolationlevel::READ_COMMITTED) {
    query = engine_->get(key, tranc_id_);
  } else {
    if (read_map_.find(key) != read_map_.end()) {
      query = read_map_[key];
    } else {
      query = engine_->get(key, tranc_id_);
      read_map_[key] = query;
    }
  }
  return query.has_value() ? std::optional<std::string>(query->first)
                           : std::nullopt;
}

bool TranContext::commit() {
  auto isolation_level = get_isolation_level();
  if (isolation_level == Isolationlevel::READ_UNCOMMITTED) {
    operations.emplace_back(Record::commitRecord(tranc_id_));
    auto res = tranManager_->write_to_wal(operations);
    if (!res) {
      throw std::runtime_error("write to wal failed");
    }
    engine_->memtable.put("", "", tranc_id_); // 触发memtable刷盘
    isCommited = true;
    tranManager_->add_ready_to_flush_tranc_id(tranc_id_,
                                              TransactionState::COMMITTED);
    return true;
  }
  MemTable &memtable = engine_->memtable;
  std::unique_lock<std::shared_mutex> wlock1(memtable.frozen_mtx);
  std::unique_lock<std::shared_mutex> wlock2(memtable.current_mtx);
  if (isolation_level == Isolationlevel::READ_COMMITTED ||
      isolation_level == Isolationlevel::REPEATABLE_READ) {
    std::shared_lock<std::shared_mutex> rlock(engine_->ssts_mtx);
    for (auto &[k, v] : temp_map_) {
      auto res = memtable.get_(k, 0);
      if (res.is_valid() && res.get_transaction_id() > tranc_id_) {
        isAborted = true;
        tranManager_->add_ready_to_flush_tranc_id(tranc_id_,
                                                  TransactionState::ABORTED);
        return false;
      } else {
        if (tranManager_->get_max_flushed_tranc_id() <= tranc_id_) {
          continue;
        }
        auto res = engine_->get(k, 0);
        if (res.has_value()) {
          auto [v, tranc_id] = res.value();
          if (tranc_id > tranc_id_) {
            isAborted = true;
            tranManager_->add_ready_to_flush_tranc_id(
                tranc_id_, TransactionState::ABORTED);
            return false;
          }
        }
      }
    }
    for (auto const &[k, v] : temp_map_) {
      memtable.put_(k, v, tranc_id_);
    }
  }
  operations.emplace_back(Record::commitRecord(tranc_id_));
  auto wal_res = tranManager_->write_to_wal(operations);
  if (!wal_res) {
    throw std::runtime_error("write to wal failed");
  }
  isCommited = true;
  tranManager_->add_ready_to_flush_tranc_id(tranc_id_,
                                            TransactionState::COMMITTED);
  return true;
}

bool TranContext::abort() {
  auto isolation_level = get_isolation_level();

  if (isolation_level == Isolationlevel::READ_UNCOMMITTED) {
    for (auto &[k, res] : rollback_map_) {
      if (res.has_value()) {
        engine_->put(k, res->first, res->second);
      } else {
        engine_->remove(k, tranc_id_);
      }
    }
  }
  tranManager_->add_ready_to_flush_tranc_id(tranc_id_,
                                            TransactionState::ABORTED);
  return true;
}
enum Isolationlevel TranContext::get_isolation_level() {
  return isolation_level_;
}

TranManager::TranManager(std::string data_dir) : data_dir_(data_dir) {
  auto file_path = get_tranc_id_file_path();

  // 判断文件是否存在

  if (!std::filesystem::exists(file_path)) {
    tranc_id_file_ = FileObj::open(file_path, true);
    flushedTrancIds_.insert(0);
  } else {
    tranc_id_file_ = FileObj::open(file_path, false);
    load_tranc_id_file();
  }
}

void TranManager::init_new_wal() {
  for (const auto &entry : std::filesystem::directory_iterator(data_dir_)) {
    if (entry.path().filename().string().find("wal.") == 0) {
      std::filesystem::remove(entry.path());
    }
  }
  wal = std::make_shared<WAL>(data_dir_, 128, get_max_flushed_tranc_id(), 1,
                              4096);
  flushedTrancIds_.clear();
  flushedTrancIds_.insert(nextTransactionId_.load() - 1);
}

void TranManager::set_engine(std::shared_ptr<LSMEngine> engine) {
  engine_ = std::move(engine);
}

TranManager::~TranManager() { write_tranc_id_file(); }

void TranManager::write_tranc_id_file() {
  int buffer_size = sizeof(uint64_t) * (flushedTrancIds_.size() + 2);
  std::vector<uint8_t> buf(buffer_size, 0);
  uint64_t nextTransactionId = nextTransactionId_.load();
  uint64_t tranc_size = flushedTrancIds_.size();
  memcpy(buf.data(), &nextTransactionId, sizeof(uint64_t));
  memcpy(buf.data() + sizeof(uint64_t), &tranc_size, sizeof(uint64_t));
  int offset = sizeof(uint64_t) * 2;
  for (auto &tranc_id : flushedTrancIds_) {
    memcpy(buf.data() + offset, &tranc_id, sizeof(uint64_t));
    offset += sizeof(uint64_t);
  }

  tranc_id_file_.write(0, buf);
  tranc_id_file_.sync();
}

void TranManager::load_tranc_id_file() {
  nextTransactionId_ = tranc_id_file_.read_uint64(0);
  uint64_t size = tranc_id_file_.read_uint64(sizeof(uint64_t));
  int offset = sizeof(uint64_t) * 2;
  for (int i = 0; i < size; i++) {
    uint64_t flushed_id = tranc_id_file_.read_uint64(offset);
    flushedTrancIds_.insert(flushed_id);
    offset += sizeof(uint64_t);
  }
}

void TranManager::add_ready_to_flush_tranc_id(uint64_t tranc_id,
                                              TransactionState state) {
  std::unique_lock lock(mutex_);
  readyToFlushTrancIds_[tranc_id] = state;
}

void TranManager::add_flushed_tranc_id(uint64_t tranc_id) {
  std::unique_lock lock(mutex_);
  std::vector<uint64_t> needRemove;
  for (auto &[readyId, state] : readyToFlushTrancIds_) {
    if (readyId < tranc_id) {
      if (state == TransactionState::COMMITTED) {
        flushedTrancIds_.insert(readyId);
      }
      needRemove.push_back(readyId);
    } else if (readyId == tranc_id) {
      flushedTrancIds_.insert(readyId);
      needRemove.push_back(readyId);
      break;
    }
  }
}

uint64_t TranManager::getNextTransactionId() {
  return nextTransactionId_.fetch_add(1);
}

std::set<uint64_t> &TranManager::get_flushed_tranc_ids() {
  return flushedTrancIds_;
}

uint64_t TranManager::get_max_flushed_tranc_id() {
  // 需保证 size 至少为1
  return *flushedTrancIds_.rbegin();
}

uint64_t TranManager::get_checkpoint_tranc_id() {
  // 需保证 size 至少为1
  return *flushedTrancIds_.begin();
}
std::shared_ptr<TranContext>
TranManager::new_tranc(const Isolationlevel &isolation_level) {

  // 获取锁
  std::unique_lock<std::mutex> lock(mutex_);

  auto tranc_id = getNextTransactionId();
  activeTrans_[tranc_id] = std::make_shared<TranContext>(
      tranc_id, engine_, shared_from_this(), isolation_level);

  return activeTrans_[tranc_id];
}
std::string TranManager::get_tranc_id_file_path() {
  if (data_dir_.empty()) {
    data_dir_ = "./";
  }
  return data_dir_ + "/tranc_id";
}

std::map<uint64_t, std::vector<Record>> TranManager::check_recover() {
  std::map<uint64_t, std::vector<Record>> wal_records =
      WAL::recover(data_dir_, *flushedTrancIds_.begin());
  return wal_records;
}
bool TranManager::write_to_wal(const std::vector<Record> &records) {
  try {
    wal->log(records, true);
  } catch (const std::exception &e) {
    return false;
  }
  return true;
}

} // namespace my_tiny_lsm