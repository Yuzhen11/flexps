#include "gtest/gtest.h"

#include "comm/fake_mailbox.hpp"
#include "task/scheduler_thread.cpp"
#include "task/worker_thread.cpp"

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
  EXPECT_EQ(msg.meta.sender, worker_thread.GetId());
  EXPECT_EQ(msg.meta.recver, scheduler_thread.GetId());
  EXPECT_EQ(msg.meta.flag, Flag::kExit);
}

}  // namespace
}  // namespace flexps
