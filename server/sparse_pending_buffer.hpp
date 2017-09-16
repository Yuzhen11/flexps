#pragma once

#include "base/message.hpp"
#include "server/abstract_pending_buffer"

#include <unordered_map>

namespace flexps {

class SparsePendingBuffer : public AbstractPendingBuffer {
public:
  virtual std::vector<Message> Pop(int clock, int tid = -1) override;
  virtual void Push(int clock, Message& message) override;
  virtual int Size(int progress) override;

private:
  std::unordered_map<int, std::unordered_map<int, Message>> buffer_;
};

}  // namespace flexps
