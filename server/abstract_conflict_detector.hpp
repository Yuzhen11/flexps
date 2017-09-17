#pragma once

#include "base/message.hpp"

namespace flexps {

class AbstractConflictDetector {
 public:
  virtual bool ConflictInfo(const third_party::SArray<char>& paramIDs, const int begin_version,
                            const int end_version) = 0;
  // TODO(Ruoyu Wu): should incldue magic.h, using key = uint32_t
  virtual void AddRecord(const int version, const third_party::SArray<char>& paramIDs) = 0;
  virtual void RemoveRecord(const int version, const third_party::SArray<char>& paramIDs) = 0;
  virtual void ClockRemoveRecord(constint version) = 0;
  virtual int RecorderSize(const int version) = 0;
};

}  // namespace flexps
