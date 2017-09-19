#pragma once

#include "base/message.hpp"

namespace flexps {

class AbstractConflictDetector {
 public:
  virtual bool ConflictInfo(const third_party::SArray<uint32_t>& paramIDs, const int begin_version,
                            const int end_version, int& forwarded_thread_id, int& forwarded_version) = 0;
  // TODO(Ruoyu Wu): should incldue magic.h, using key = uint32_t
  virtual void AddRecord(const int version, const uint32_t tid, const third_party::SArray<uint32_t>& paramIDs) = 0;
  virtual void RemoveRecord(const int version, const uint32_t tid, const third_party::SArray<uint32_t>& paramIDs) = 0;
  virtual void ClockRemoveRecord(const int version) = 0;
};

}  // namespace flexps
