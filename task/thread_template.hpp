#pragma once

#include "base/message.hpp"
#include "base/threadsafe_queue.hpp"
#include "comm/abstract_mailbox.hpp"
#include "glog/logging.h"

#include <thread>

namespace flexps {

class ThreadTemplate {
 public:
  ThreadTemplate(uint32_t id, AbstractMailbox* const mailbox) : id_(id), mailbox_(mailbox) {
    mailbox_->RegisterQueue(id_, &work_queue_);
  }

  virtual ~ThreadTemplate() { mailbox_->DeregisterQueue(id_); }

  virtual void Start() {
    work_thread_ = std::thread([this] { Main(); });
  }

  virtual void Stop() { work_thread_.join(); }

  virtual void Send(const Message& msg) { mailbox_->Send(msg); }

  ThreadsafeQueue<Message>* GetWorkQueue() { return &work_queue_; }

  uint32_t GetId() const { return id_; }

  virtual void Main() {
    while (true) {
      Message msg;
      work_queue_.WaitAndPop(&msg);
      CHECK(msg.meta.recver == id_);
      LOG(INFO) << msg.DebugString();
      if (msg.meta.flag == Flag::kExit)
        break;
    }
  }

  uint32_t id_;
  std::thread work_thread_;
  ThreadsafeQueue<Message> work_queue_;
  // not owned
  AbstractMailbox* const mailbox_;
};
}  // namespace flexps
