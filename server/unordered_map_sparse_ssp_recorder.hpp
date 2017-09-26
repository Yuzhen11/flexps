#pragma once

#include "server/abstract_sparse_ssp_recorder.hpp"
#include "glog/logging.h"

#include <unordered_map>
#include <queue>

namespace flexps {

class UnorderedMapSparseSSPRecorder : public AbstractSparseSSPRecorder {
public:
  UnorderedMapSparseSSPRecorder(uint32_t staleness, uint32_t speculation) : staleness_(staleness), speculation_(speculation) {}

  virtual void AddRecord(Message& msg) override;
  virtual void RemoveRecord(const int version, const uint32_t tid,
                       const third_party::SArray<uint32_t>& paramIDs) override;
  virtual void ClockRemoveRecord(const int version) override;
  virtual bool HasConflict(const third_party::SArray<uint32_t>& paramIDs, const int begin_version,
                            const int end_version, int& forwarded_thread_id, int& forwarded_version) override;

  virtual void HandleFutureKeys(int progress, int sender) override;

  int ParamSize(const int version);
  int WorkerSize(const int version);
  int TotalSize(const int version);

private:
  uint32_t staleness_;
  uint32_t speculation_;

  std::unordered_map<int, std::unordered_map<uint32_t, std::set<uint32_t>>> main_recorder_;

  // <thread_id, [<version, key>]>, has at most speculation_ + 1 queue size for each thread_id
  std::unordered_map<int, std::queue<std::pair<int, third_party::SArray<Key>>>> future_keys_;
};

}  // namespace flexps
