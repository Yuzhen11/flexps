#pragma once

#include "server/abstract_sparse_ssp_recorder_2.hpp"
#include "glog/logging.h"

#include <unordered_map>
#include <queue>

namespace flexps {

class UnorderedMapSparseSSPRecorder2 : public AbstractSparseSSPRecorder2 {
public:
  UnorderedMapSparseSSPRecorder2(uint32_t staleness, uint32_t speculation) : staleness_(staleness), speculation_(speculation) {}
  virtual std::vector<Message> GetNonConflictMsgs(int progress, int sender, int min_clock) override;
  virtual std::vector<Message> HandleTooFastBuffer(int min_clock) override;
  virtual void RemoveRecord(int version) override;
  virtual void AddRecord(Message& msg) override;

private:
  std::vector<Message> RemoveRecordAndGetNonConflictMsgs(int version, int min_clock, uint32_t tid,
                       const third_party::SArray<uint32_t>& keys);

  bool HasConflict(const third_party::SArray<uint32_t>& keys, const int begin_version,
                   const int end_version, int* forwarded_key, int* forwarded_version);

  uint32_t staleness_;
  uint32_t speculation_;

  // <version, <key, {count, [msg]}>>
  std::unordered_map<int, std::unordered_map<uint32_t, std::pair<uint32_t, std::vector<Message>>>> main_recorder_;

  // <thread_id, [<version, key>]>, has at most speculation_ + 1 queue size for each thread_id
  std::unordered_map<int, std::queue<std::pair<int, third_party::SArray<Key>>>> future_keys_;

  // <thread_id, [<version, msg>]>
  std::unordered_map<int, std::queue<std::pair<int, Message>>> future_msgs_;

  std::vector<Message> too_fast_buffer_;
};

}  // namespace flexps
