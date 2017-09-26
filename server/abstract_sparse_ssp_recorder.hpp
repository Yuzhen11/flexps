#pragma once

#include "base/message.hpp"

namespace flexps {

class AbstractSparseSSPRecorder {
public:
  virtual void AddRecord(Message& msg) = 0;
  virtual void RemoveRecord(const int version, const uint32_t tid,
                       const third_party::SArray<uint32_t>& paramIDs) = 0;
  virtual void ClockRemoveRecord(const int version) = 0;
  virtual bool HasConflict(const third_party::SArray<uint32_t>& paramIDs, const int begin_version,
                            const int end_version, int& forwarded_thread_id, int& forwarded_version) = 0;

  // TODO(Ruoyu Wu): virtual or not?
  virtual void HandleFutureKeys(int progress, int sender) = 0;
};

}  // namespace flexps
