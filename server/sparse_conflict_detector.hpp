#pragma once

#include "base/message.hpp"
#include "server/abstract_conflict_detector.hpp"
#include "server/abstract_pending_buffer.hpp"

#include <set>
#include <unordered_map>

namespace flexps {

class SparseConflictDetector : public AbstractConflictDetector {
 public:
  SparseConflictDetector() = delete;

  virtual bool ConflictInfo(const third_party::SArray<char>& paramIDs, const int begin_version,
                            const int end_version) override;
  virtual void AddRecord(const int version, const third_party::SArray<char>& paramIDs) override;
  virtual void RemoveRecord(const int version, const third_party::SArray<char>& paramIDs) override;
  virtual void ClockRemoveRecord(const int version) override;
  virtual int RecorderSize(const int version) override;

 private:
  // Should be cleared
  // <clock, <paramID, count>>
  std::unordered_map<int, unordered_map<uint32_t, int>> recorder_;
};

}  // namespace flexps
