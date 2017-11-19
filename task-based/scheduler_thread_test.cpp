#include "glog/logging.h"
#include "gtest/gtest.h"

#include "task-based/scheduler_thread.hpp"
#include "task-based/worker_thread.hpp"
#include "comm/abstrct_mailbox.hpp"

#include <mutex>

namespace flexps {
namespace {

class TestSchedulerThread : public testing::Test {
 public:
  TestSchedulerThread() {}
  ~TestSchedulerThread() {}

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

TEST_F(TestSchedulerThread, Init) {
  FakeMailbox mailbox;
  SchedulerThread scheduler_thread(mailbox);
}

TEST_F(TestSchedulerThread, StartStop) {
  FakeMailbox mailbox;
  SchedulerThread scheduler_thread(mailbox);
  auto* work_queue = scheduler_thread.GetWorkQueue();
  scheduler_thread.Start();

  Message exit_msg;
  exit_msg.meta.flag = Flag::kExit;
  work_queue->Push(exit_msg);
  scheduler_thread.Stop();
}

TEST_F(TestSchedulerThread, SendToAllWorkers) {
  FakeMailbox mailbox;
  std::vector<WorkerThread> worker_threads;
  SchedulerThread scheduler_thread(mailbox);

  for (int i = 1; i <= 3; i++) {
    worker_threads.push_back(new WorkerThread(i, mailbox));
    scheduler_thread.RegisterWorker(i);
  }
  std::vector<ThreadsafeQueue<Message>*> worker_queues = {worker_threads[0].GetWorkQueue(), worker_threads[1].GetWorkQueue(), worker_threads[2].GetWorkQueue()};
  
  auto worker_ids = scheduler_thread.GetWorkerIDs();
  Message send_msg;
  send_msg.meta.sender = scheduler_thread.GetSchedulerId();
  send_msg.meta.flag = Flag::kExit;

  for (auto id : worker_ids) {
    send_msg.meta.recver = id;
    scheduler_thread.Send(send_msg);

    Message msg;
    worker_queues[id - 1].WaitAndPop(&msg);
    CHECK(msg.meta.sender == scheduler_thread.GetSchedulerId());
    CHECK(msg.meta.recver == scheduler_thread.GetWorkerId());
    CHECK(msg.meta.flag == Flag::kExit);
  }
}

}  // namespace
}  // namespace flexps
