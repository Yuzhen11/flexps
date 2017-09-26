#pragma once

#include "base/message.hpp"

#include <list>

namespace flexps {

class AbstractSparseSSPRecorder {
public:
  // for conflict detection use
  virtual void AddRecord(Message& msg) = 0;
  virtual void RemoveRecord(const int version, const uint32_t tid,
                       const third_party::SArray<uint32_t>& paramIDs) = 0;
  virtual void ClockRemoveRecord(const int version) = 0;
  virtual bool HasConflict(const third_party::SArray<uint32_t>& paramIDs, const int begin_version,
                            const int end_version, int& forwarded_thread_id, int& forwarded_version) = 0;

  // Msg related operation
  virtual std::list<Message> PopMsg(const int version, const int tid) = 0;
  virtual void PushMsg(const int version, Message& message, const int tid) = 0;
  virtual void EraseMsgBuffer(int version) = 0;
  virtual int MsgBufferSize(const int version) = 0;

  // Exceptinoal cases
  virtual void HandleTooFastBuffer(int updated_min_clock, int min_clock, std::list<Message>& rets) = 0;
  virtual void HandleFutureKeys(int progress, int sender) = 0;
  virtual void PushBackTooFastBuffer(Message& msg) = 0;
};

}  // namespace flexps
