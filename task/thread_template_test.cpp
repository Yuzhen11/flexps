#include "gtest/gtest.h"

#include "comm/fake_mailbox.hpp"
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
  EXPECT_EQ(msg.meta.sender, thread0.GetId());
  EXPECT_EQ(msg.meta.recver, thread1.GetId());
  EXPECT_EQ(msg.meta.flag, Flag::kExit);
}

}  // namespace
}  // namespace flexps
