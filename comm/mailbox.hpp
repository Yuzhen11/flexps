#pragma once

#include "base/node.hpp"
#include "base/threadsafe_queue.hpp"
#include "base/message.hpp"

#include <atomic>
#include <map>
#include <thread>
#include <vector>
#include <unordered_map>

#include <zmq.h>

namespace flexps {

class Mailbox {
 public:
  Mailbox(const Node& node): node_(node) {}
  void RegisterQueue(uint32_t queue_id, ThreadsafeQueue<Message>* const queue);
  int Send(const Message& msg);
  int Recv(Message* msg);
  void Start(const std::vector<Node>& nodes);
  void Stop();
  size_t GetQueueMapSize() const;

 private:
  void Connect(const Node& node);
  void Bind(const Node& node);

  void Receiving();

  std::atomic<bool> ready{false};
  std::map<uint32_t, ThreadsafeQueue<Message>* const> queue_map_;

  std::thread receiver_thread_;
  Node node_;
  void* context_ = nullptr;
  std::unordered_map<uint32_t, void*> senders_;
  void* receiver_ = nullptr;
  std::mutex lock_;
};

}  // namespace flexps
