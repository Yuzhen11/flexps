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
  Mailbox(const Node& node, const std::vector<Node>& nodes, AbstractIdMapper* id_mapper);
  virtual void RegisterQueue(uint32_t queue_id, ThreadsafeQueue<Message>* const queue) override;
  virtual void DeregisterQueue(uint32_t queue_id) override;
  virtual int Send(const Message& msg) override;
  virtual void Barrier() override;
  int Recv(Message* msg);
  void Start();
  void Stop();
  size_t GetQueueMapSize() const;

  // For testing only
  void ConnectAndBind();
  void StartReceiving();
  void StopReceiving();
  void CloseSockets();
 private:
  void Connect(const Node& node);
  void Bind(const Node& node);

  void Receiving();

  std::map<uint32_t, ThreadsafeQueue<Message>* const> queue_map_;
  // Not owned
  AbstractIdMapper* id_mapper_;

  std::thread receiver_thread_;

  // node
  Node node_;
  std::vector<Node> nodes_;

  // socket
  void* context_ = nullptr;
  std::unordered_map<uint32_t, void*> senders_;
  void* receiver_ = nullptr;
  std::mutex mu_;

  // barrier
  std::mutex barrier_mu_;
  std::condition_variable barrier_cond_;
  uint32_t this_count_ = 0;
  uint32_t next_count_ = 0;
  uint32_t progress_ = 0;
};

}  // namespace flexps
