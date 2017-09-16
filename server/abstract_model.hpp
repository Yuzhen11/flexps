#pragma once

#include <cinttypes>
#include "base/message.hpp"
#include "base/threadsafe_queue.hpp"

namespace flexps {

class AbstractModel {
 public:
  virtual void Clock(Message& msg) = 0;
  virtual void Add(Message& msg) = 0;
  virtual void Get(Message& msg) = 0;
  virtual int GetProgress(int tid) = 0;
  virtual void ResetWorker(Message& msg) = 0;
  virtual ~AbstractModel() {}
};

}  // namespace flexps
