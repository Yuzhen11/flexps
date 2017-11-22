#pragma once

#include "glog/logging.h"

#include "comm/abstract_mailbox.hpp"

#include <mutex>

namespace flexps {

class FakeMailbox : public AbstractMailbox {
 public:
  virtual void RegisterQueue(uint32_t queue_id, ThreadsafeQueue<Message>* const queue) override {
    std::lock_guard<std::mutex> lk(mu_);
    CHECK(queue_map_.find(queue_id) == queue_map_.end()) << "Queue " << queue_id << "already registered";
    queue_map_.insert({queue_id, queue});
  };

  virtual void DeregisterQueue(uint32_t queue_id) override {
    std::lock_guard<std::mutex> lk(mu_);
    CHECK(queue_map_.find(queue_id) != queue_map_.end()) << "Queue " << queue_id << "is not in mailbox";
    queue_map_.erase(queue_id);
  };

  virtual int Send(const Message& msg) override { queue_map_[msg.meta.recver]->Push(std::move(msg)); };

  virtual void Barrier() {}

  std::mutex mu_;
  std::map<uint32_t, ThreadsafeQueue<Message>* const> queue_map_;
};

}  // namespace flexps
