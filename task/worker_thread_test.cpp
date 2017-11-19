#include "glog/logging.h"
#include "gtest/gtest.h"

#include "task-based/scheduler_thread.hpp"
#include "task-based/worker_thread.hpp"
#include "comm/abstract_mailbox.hpp"

#include <mutex>

namespace flexps {
namespace {

class TestWorkerThread : public testing::Test {
 public:
  TestWorkerThread() {}
  ~TestWorkerThread() {}

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

    virtual int Send(const Message& msg) override {
      queue_map_[msg.meta.recver]->Push(std::move(msg));
    };

  private:
    std::mutex mu_;
    std::map<uint32_t, ThreadsafeQueue<Message>* const> queue_map_;
};

TEST_F(TestWorkerThread, SendToScheduler) {

  FakeMailbox mailbox;
  SchedulerThread scheduler_thread(&mailbox);
  WorkerThread worker_thread(1, &mailbox);
  scheduler_thread.RegisterWorker(1);
  auto* sheduler_queue = scheduler_thread.GetWorkQueue();
  
  Message send_msg;
  send_msg.meta.sender = worker_thread.GetId();
  send_msg.meta.recver = scheduler_thread.GetId();
  send_msg.meta.flag = Flag::kExit;
  worker_thread.Send(send_msg);

    Message msg;
    sheduler_queue->WaitAndPop(&msg);
    CHECK(msg.meta.sender == worker_thread.GetId());
    CHECK(msg.meta.recver == scheduler_thread.GetId());
    CHECK(msg.meta.flag == Flag::kExit);
}

}  // namespace
}  // namespace flexps
