#pragma once

#include "base/message.hpp"

#include <vector>

namespace flexps {

class AbstractPendingBuffer {
 public:
  virtual std::vector<Message> Pop(int clock, int tid = -1) = 0;
  virtual void Push(int clock, Message& message, int tid = -1) = 0;
  virtual int Size(int progress) = 0;
};

}  // namespace flexps
