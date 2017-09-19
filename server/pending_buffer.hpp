#pragma once

#include "base/message.hpp"
#include "server/abstract_pending_buffer.hpp"

#include <unordered_map>

namespace flexps {

class PendingBuffer : public AbstractPendingBuffer {
 public:
  virtual std::vector<Message> Pop(const int clock, const int tid = -1) override;
  virtual void Push(const int clock, Message& message, const int tid = -1) override;
  virtual int Size(const int progress) override;

 private:
  // TODO(Ruoyu Wu): each vec of buffer_ should be pop one by one, now it is not guaranteed
  std::unordered_map<int, std::vector<Message>> buffer_;
};

}  // namespace flexps
