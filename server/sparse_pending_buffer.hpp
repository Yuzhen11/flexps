#pragma once

#include "base/message.hpp"
#include "server/abstract_pending_buffer.hpp"

#include <unordered_map>

namespace flexps {

class SparsePendingBuffer : public AbstractPendingBuffer {
 public:
  virtual std::vector<Message> Pop(const int version, const int tid = -1) override;
  virtual void Push(const int version, Message& message, const int tid = -1) override;
  virtual int Size(const int version) override;

 private:
  // <version, <tid, Message>>
  // TODO(Ruoyu Wu): There should be more efficient data structure
  std::unordered_map<int, std::unordered_map<int, std::vector<Message>>> buffer_;
};

}  // namespace flexps
