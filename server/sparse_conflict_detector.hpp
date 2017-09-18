#pragma once

#include "base/message.hpp"
#include "server/abstract_conflict_detector.hpp"

#include <unordered_map>
#include <set>

namespace flexps {

class SparseConflictDetector : public AbstractConflictDetector {
 public:
  virtual bool ConflictInfo(const third_party::SArray<uint32_t>& paramIDs, const int begin_version,
                            const int end_version, int& forwarded_thread_id, int& forwarded_version) override;
  virtual void AddRecord(const int version, const uint32_t tid, const third_party::SArray<uint32_t>& paramIDs) override;
  virtual void RemoveRecord(const int version, const uint32_t tid, const third_party::SArray<uint32_t>& paramIDs) override;
  virtual void ClockRemoveRecord(const int version) override;

  int ParamSize(const int version);
  int WorkerSize(const int version);
  int TotalSize(const int version);

 private:
  // TODO(Ruoyu Wu): Not the best data structrue
  // <clock, <paramID, <thread_id>>>
  std::unordered_map<int, std::unordered_map<uint32_t, std::set<uint32_t>>> recorder_;
};

}  // namespace flexps
