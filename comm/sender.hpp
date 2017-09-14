#pragma once

#include "base/threadsafe_queue.hpp"
#include "comm/abstract_sender.hpp"
#include "comm/abstract_mailbox.hpp"

#include <thread>

namespace flexps {

class Sender : public AbstractSender {
 public:
  explicit Sender(AbstractMailbox* mailbox);
  virtual void Start() override;
  virtual void Send() override;
  virtual void Stop() override;
  ThreadsafeQueue<Message>* GetMessageQueue();

 private:
  ThreadsafeQueue<Message> send_message_queue_;
  // Not owned
  AbstractMailbox* mailbox_;
  std::thread sender_thread_;
};

}  // namespace flexps
