#pragma once

#include "base/threadsafe_queue.hpp"
#include "base/abstract_id_mapper.hpp"
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
  Mailbox(const Node& node, const std::vector<Node>& nodes, AbstractIdMapper* id_mapper)
    : node_(node), nodes_(nodes), id_mapper_(id_mapper) {}
  void RegisterQueue(uint32_t queue_id, ThreadsafeQueue<Message>* const queue);
  virtual int Send(const Message& msg) override;
  virtual int Recv(Message* msg) override;
  virtual void Start() override;
  virtual void Stop() override;
  size_t GetQueueMapSize() const;

 private:
  virtual void Connect(const Node& node) override;
  virtual void Bind(const Node& node) override;

  virtual void Receiving() override;

  std::map<uint32_t, ThreadsafeQueue<Message>* const> queue_map_;
  // Not owned
  AbstractIdMapper* id_mapper_;

  std::thread receiver_thread_;

  // node
  Node node_;
  std::vector<Node> nodes_;
  int finish_count_ = 0;

  // socket
  void* context_ = nullptr;
  std::unordered_map<uint32_t, void*> senders_;
  void* receiver_ = nullptr;
  std::mutex mu_;
};

}  // namespace flexps
