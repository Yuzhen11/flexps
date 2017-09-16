#pragma once

#include "base/message.hpp"
#include "server/abstract_pending_buffer"

#include <unordered_map>

namespace flexps {

class PendingBuffer : public AbstractPendingBuffer {
 public:
  virtual std::vector<Message> Pop(int clock, int tid = -1) override;
  virtual void Push(int clock, Message& message, int tid = -1) override;
  virtual int Size(int progress) override;

 private:
  // TODO(Ruoyu Wu): each vec of buffer_ should be pop one by one, now it is not guaranteed
  std::unordered_map<int, std::vector<Message>> buffer_;
};

}  // namespace flexps
