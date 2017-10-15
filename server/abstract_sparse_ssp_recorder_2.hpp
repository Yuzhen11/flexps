#pragma once

#include "base/message.hpp"

#include <list>

namespace flexps {

class AbstractSparseSSPRecorder2 {
public:
  virtual void GetNonConflictMsgs(int progress, int sender, int min_clock, std::vector<Message>* const msgs) = 0;
  virtual void HandleTooFastBuffer(int min_clock, std::vector<Message>* const msgs) = 0;
  virtual void RemoveRecord(int version) = 0;
  virtual void AddRecord(Message& msg) = 0;
  virtual ~AbstractSparseSSPRecorder2() {}

};

}  // namespace flexps
