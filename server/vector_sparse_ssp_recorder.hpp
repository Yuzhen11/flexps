#pragma once

#include "base/third_party/range.h"
#include "server/abstract_sparse_ssp_recorder.hpp"
#include "glog/logging.h"

#include <unordered_map>
#include <queue>
#include <vector>

namespace flexps {

class VectorSparseSSPRecorder : public AbstractSparseSSPRecorder {
public:
  VectorSparseSSPRecorder(uint32_t staleness, uint32_t speculation, third_party::Range range);

  virtual void AddRecord(Message& msg) override;
  virtual void RemoveRecord(const int version, const uint32_t tid,
                       const third_party::SArray<uint32_t>& paramIDs) override;
  virtual void ClockRemoveRecord(const int version) override;
  virtual bool HasConflict(const third_party::SArray<uint32_t>& paramIDs, const int begin_version,
                            const int end_version, int& forwarded_thread_id, int& forwarded_version) override;

  virtual std::list<Message> PopMsg(const int version, const int tid) override;
  virtual void PushMsg(const int version, Message& message, const int tid) override;
  virtual void EraseMsgBuffer(int version) override;
  virtual int MsgBufferSize(const int version) override;

  virtual void HandleTooFastBuffer(int updated_min_clock, int min_clock, std::list<Message>& rets) override;
  virtual void HandleFutureKeys(int progress, int sender) override;
  virtual void PushBackTooFastBuffer(Message& msg) override;

private:
  uint32_t staleness_;
  uint32_t speculation_;

  third_party::Range range_;

  uint32_t main_recorder_version_level_size_ = 0;

  // <version, <key, [tid]>>
  std::vector<std::vector<std::set<uint32_t>>> main_recorder_;

  // <version, <thread_id, [msg]>>
  std::unordered_map<int, std::unordered_map<int, std::list<Message>>> buffer_;

  // <thread_id, [<version, key>]>, has at most speculation_ + 1 queue size for each thread_id
  std::unordered_map<int, std::queue<std::pair<int, third_party::SArray<Key>>>> future_keys_;

  std::vector<Message> too_fast_buffer_;
};

}  // namespace flexps
