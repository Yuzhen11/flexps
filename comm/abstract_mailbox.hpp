#pragma once

#include "base/message.hpp"
#include "base/node.hpp"
#include "base/threadsafe_queue.hpp"

namespace flexps {

class AbstractMailbox {
 public:
  virtual ~AbstractMailbox() = default;
  virtual int Send(const Message& msg) = 0;
  virtual void RegisterQueue(uint32_t queue_id, ThreadsafeQueue<Message>* const queue) = 0;
  virtual void DeregisterQueue(uint32_t queue_id) = 0;
  virtual void Barrier() = 0;
};

}  // namespace flexps
