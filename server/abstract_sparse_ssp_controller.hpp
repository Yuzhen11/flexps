#pragma once

#include "base/message.hpp"

#include <list>

namespace flexps {

class AbstractSparseSSPController {
 public:
  virtual ~AbstractSparseSSPController() {}
  virtual std::list<Message> UnblockRequests(int progress, int sender, int updated_min_clock, int min_clock) = 0;
  virtual void AddRecord(Message& msg) = 0;

  // get_buffer's func
  virtual std::list<Message> Pop(const int version, const int tid = -1) = 0;
  virtual std::list<Message>& Get(const int version, const int tid = -1) = 0;
  virtual void Push(const int version, Message& message, const int tid = -1) = 0;
  virtual int Size(const int version) = 0;


  // recorder's func
  virtual bool ConflictInfo(const third_party::SArray<uint32_t>& paramIDs, const int begin_version,
                            const int end_version, int& forwarded_thread_id, int& forwarded_version) = 0;
  virtual void AddRecord(const int version, const uint32_t tid, const third_party::SArray<uint32_t>& paramIDs) = 0;
  virtual void RemoveRecord(const int version, const uint32_t tid,
                            const third_party::SArray<uint32_t>& paramIDs) = 0;
  virtual void ClockRemoveRecord(const int version) = 0;
};


}  // namespace flexps

