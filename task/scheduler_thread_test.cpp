#include "gtest/gtest.h"

#include "comm/fake_mailbox.hpp"
#include "task/scheduler_thread.cpp"
#include "task/worker_thread.cpp"

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

TEST_F(TestSchedulerThread, Init) {
  FakeMailbox mailbox;
  SchedulerThread scheduler_thread(&mailbox);
}

TEST_F(TestSchedulerThread, SendToAllWorkers) {
  FakeMailbox mailbox;
  SchedulerThread scheduler_thread(&mailbox);
  std::vector<WorkerThread*> worker_threads;
  std::vector<ThreadsafeQueue<Message>*> worker_queues;

  for (int i = 1; i <= 3; i++) {
    worker_threads.push_back(new WorkerThread(i, &mailbox));
    worker_queues.push_back(worker_threads[i - 1]->GetWorkQueue());
    scheduler_thread.RegisterWorker(i);
  }

  auto worker_ids = scheduler_thread.GetWorkerIds();
  Message send_msg;
  send_msg.meta.sender = scheduler_thread.GetId();
  send_msg.meta.flag = Flag::kExit;

  for (auto id : worker_ids) {
    send_msg.meta.recver = id;
    scheduler_thread.Send(send_msg);

    Message msg;
    worker_queues[id - 1]->WaitAndPop(&msg);
    EXPECT_EQ(msg.meta.sender, scheduler_thread.GetId());
    EXPECT_EQ(msg.meta.recver, worker_threads[id - 1]->GetId());
    EXPECT_EQ(msg.meta.flag, Flag::kExit);
  }
}

}  // namespace
}  // namespace flexps
