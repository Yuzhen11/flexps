#pragma once

#include "base/message.hpp"

#include <vector>

namespace flexps {

class AbstractPendingBuffer {
 public:
  virtual std::vector<Message> Pop(const int clock, const int tid = -1) = 0;
  virtual void Push(const int clock, Message& message, const int tid = -1) = 0;
  virtual int Size(const int progress) = 0;
};

}  // namespace flexps
