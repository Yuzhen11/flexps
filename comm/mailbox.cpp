#include "comm/mailbox.hpp"

#include "glog/logging.h"

namespace flexps {

void Mailbox::Start(const std::vector<Node>& nodes) {
  context_ = zmq_ctx_new();
  CHECK(context_ != nullptr) << "create zmq context failed";
  zmq_ctx_set(context_, ZMQ_MAX_SOCKETS, 65536);

  Bind(node_);
  for (const auto& node : nodes) {
    Connect(node);
  }

  receiver_thread_ = std::thread(&Mailbox::Receiving, this);
}

void Mailbox::Stop() {
  receiver_thread_.join();
}

void Mailbox::Connect(const Node& node) {
  auto it = senders_.find(node.id);
  if (it != senders_.end()) {
    zmq_close(it->second);
  }
  void* sender = zmq_socket(context_, ZMQ_DEALER);
  CHECK(sender != nullptr) << zmq_strerror(errno);
  std::string my_id = "ps" + std::to_string(node_.id);
  zmq_setsockopt(sender, ZMQ_IDENTITY, my_id.data(), my_id.size());
  std::string addr = "tcp://" + node.hostname + ":" + std::to_string(node.port);
  if (zmq_connect(sender, addr.c_str()) != 0) {
    LOG(FATAL) << "connect to " + addr + " failed: " << zmq_strerror(errno);
  }
  senders_[node.id] = sender;
}

void Mailbox::Bind(const Node& node) {
  receiver_ = zmq_socket(context_, ZMQ_ROUTER);
  CHECK(receiver_ != nullptr) << "create receiver socket failed: " << zmq_strerror(errno);
  std::string address = "tcp://*:" + std::to_string(node.port);
  if (zmq_bind(receiver_, address.c_str()) != 0) {
    LOG(FATAL) << "bind to " + address + " failed: " << zmq_strerror(errno);
  }
}

void Mailbox::RegisterQueue(uint32_t queue_id, ThreadsafeQueue<Message>* const queue) {
  CHECK(queue_map_.find(queue_id) != queue_map_.end());
  queue_map_.insert({queue_id, queue});
}

void Mailbox::Receiving() {
  while (true) {
    Message msg;
  }
}

}  // namespace flexps
