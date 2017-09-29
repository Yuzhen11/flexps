#pragma once

#include "base/message.hpp"

#include <list>

namespace flexps {

class AbstractSparseSSPRecorder2 {
public:
  virtual std::vector<Message> GetNonConflictMsgs(int progress, int sender, int min_clock) = 0;
  virtual std::vector<Message> HandleTooFastBuffer(int min_clock) = 0;
  virtual void RemoveRecord(int version) = 0;
  virtual void AddRecord(Message& msg) = 0;

};

}  // namespace flexps
