#include "glog/logging.h"
#include "gtest/gtest.h"

#include "comm/abstract_mailbox.hpp"
#include "task/thread_template.hpp"

#include <mutex>

namespace flexps {
namespace {

class TestThreadTemplate : public testing::Test {
 public:
  TestThreadTemplate() {}
  ~TestThreadTemplate() {}

 protected:
  void SetUp() {}
  void TearDown() {}
};

class FakeMailbox : public AbstractMailbox {
 public:
  virtual void RegisterQueue(uint32_t queue_id, ThreadsafeQueue<Message>* const queue) override {
    std::lock_guard<std::mutex> lk(mu_);
    CHECK(queue_map_.find(queue_id) == queue_map_.end());
    queue_map_.insert({queue_id, queue});
  };

  virtual void DeregisterQueue(uint32_t queue_id) override {
    std::lock_guard<std::mutex> lk(mu_);
    CHECK(queue_map_.find(queue_id) != queue_map_.end());
    queue_map_.erase(queue_id);
  };

  virtual int Send(const Message& msg) override { queue_map_[msg.meta.recver]->Push(std::move(msg)); };

 private:
  std::mutex mu_;
  std::map<uint32_t, ThreadsafeQueue<Message>* const> queue_map_;
};

TEST_F(TestThreadTemplate, Init) {
  FakeMailbox mailbox;
  ThreadTemplate thread(0, &mailbox);
}

TEST_F(TestThreadTemplate, StartStop) {
  FakeMailbox mailbox;
  ThreadTemplate thread(0, &mailbox);
  auto* work_queue = thread.GetWorkQueue();
  thread.Start();

  Message exit_msg;
  exit_msg.meta.recver = thread.GetId();
  exit_msg.meta.flag = Flag::kExit;
  work_queue->Push(exit_msg);
  thread.Stop();
}

TEST_F(TestThreadTemplate, Send) {
  FakeMailbox mailbox;
  ThreadTemplate thread0(0, &mailbox);
  ThreadTemplate thread1(1, &mailbox);

  auto* thread1_work_queue = thread1.GetWorkQueue();

  Message send_msg;
  send_msg.meta.sender = thread0.GetId();
  send_msg.meta.recver = thread1.GetId();
  send_msg.meta.flag = Flag::kExit;
  thread0.Send(send_msg);

  Message msg;
  thread1_work_queue->WaitAndPop(&msg);
  CHECK(msg.meta.sender == thread0.GetId());
  CHECK(msg.meta.recver == thread1.GetId());
  CHECK(msg.meta.flag == Flag::kExit);
}

}  // namespace
}  // namespace flexps
