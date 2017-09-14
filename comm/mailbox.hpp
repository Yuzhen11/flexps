#pragma once

#include "base/threadsafe_queue.hpp"
#include "comm/abstract_mailbox.hpp"

#include <atomic>
#include <map>
#include <thread>
#include <unordered_map>
#include <vector>

#include <zmq.h>

namespace flexps {

class Mailbox : public AbstractMailbox {
 public:
  Mailbox(const Node& node, const std::vector<Node>& nodes) : node_(node), nodes_(nodes) {}
  void RegisterQueue(uint32_t queue_id, ThreadsafeQueue<Message>* const queue);
  virtual int Send(const Message& msg);
  virtual int Recv(Message* msg);
  virtual void Start();
  virtual void Stop();
  size_t GetQueueMapSize() const;

 private:
  virtual void Connect(const Node& node);
  virtual void Bind(const Node& node);

  virtual void Receiving();

  std::map<uint32_t, ThreadsafeQueue<Message>* const> queue_map_;

  std::thread receiver_thread_;
  Node node_;
  std::vector<Node> nodes_;
  int finish_count_ = 0;
  void* context_ = nullptr;
  std::unordered_map<uint32_t, void*> senders_;
  void* receiver_ = nullptr;
  std::mutex mu_;
};

}  // namespace flexps
