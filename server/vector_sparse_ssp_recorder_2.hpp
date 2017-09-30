#pragma once

#include "server/abstract_sparse_ssp_recorder_2.hpp"
#include "glog/logging.h"

#include <unordered_map>
#include <queue>

#ifdef USE_TIMER
#include <chrono>
#endif

namespace flexps {

class VectorSparseSSPRecorder2 : public AbstractSparseSSPRecorder2 {
public:
  VectorSparseSSPRecorder2(uint32_t staleness, uint32_t speculation, third_party::Range range);
  ~VectorSparseSSPRecorder2();
  virtual void GetNonConflictMsgs(int progress, int sender, int min_clock, std::vector<Message>* const msgs) override;
  virtual void HandleTooFastBuffer(int min_clock, std::vector<Message>* const msgs) override;
  virtual void RemoveRecord(int version) override;
  virtual void AddRecord(Message& msg) override;

private:
  void RemoveRecordAndGetNonConflictMsgs(int version, int min_clock, uint32_t tid,
                       const third_party::SArray<uint32_t>& keys, std::vector<Message>* msgs);

  bool HasConflict(const third_party::SArray<uint32_t>& keys, const int begin_version,
                   const int end_version, int* forwarded_key, int* forwarded_version);

  uint32_t staleness_;
  uint32_t speculation_;

  // <version, <key, {count, [msg]}>>
  std::vector<std::vector<std::pair<uint32_t, std::vector<Message>>>> main_recorder_;

  uint32_t main_recorder_version_level_size_ = 0;
  third_party::Range range_;

  // <thread_id, [<version, key>]>, has at most speculation_ + 1 queue size for each thread_id
  std::unordered_map<int, std::queue<std::pair<int, third_party::SArray<Key>>>> future_keys_;

  // <thread_id, [<version, msg>]>
  std::unordered_map<int, std::queue<std::pair<int, Message>>> future_msgs_;

  std::vector<Message> too_fast_buffer_;

  // timer
#ifdef USE_TIMER
  std::chrono::microseconds add_record_time_{0};
  int key_count_ = 0;
  std::chrono::microseconds remove_record_time_{0};
  std::chrono::microseconds handle_own_get_time_{0};
  int by_staleness_ = 0;
  int forward_ = 0;
  int total_forward_ = 0;
  int by_speculation_ = 0;
  int too_fast_ = 0;
#endif
};

}  // namespace flexps
