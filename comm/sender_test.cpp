#include "glog/logging.h"
#include "gtest/gtest.h"

#include "comm/sender.hpp"

#include <iostream>
#include <vector>

namespace flexps {
namespace {

class TestSender : public testing::Test {
 public:
  TestSender() {}
  ~TestSender() {}

 protected:
  void SetUp() {}
  void TearDown() {}
};

class FakeMailbox : public AbstractMailbox {
 public:
  virtual int Send(const Message& msg) override {
    to_send_.Push(msg);
    return -1;
  }

  virtual void RegisterQueue(uint32_t queue_id, ThreadsafeQueue<Message>* const queue) override {}
  virtual void DeregisterQueue(uint32_t queue_id) override {}

  void WaitAndPop(Message* msg) {
    to_send_.WaitAndPop(msg);
  }
  virtual void Barrier() {}
 private:
  ThreadsafeQueue<Message> to_send_;
};

TEST_F(TestSender, StartStop) {
  FakeMailbox mailbox;
  Sender sender(&mailbox);
  sender.Start();
  sender.Stop();
}

TEST_F(TestSender, Send) {
  FakeMailbox mailbox;
  Sender sender(&mailbox);
  sender.Start();
  auto* send_queue = sender.GetMessageQueue();

  // Msg
  Message msg;
  msg.meta.sender = 123;
  msg.meta.recver = 0;
  msg.meta.model_id = 0;
  msg.meta.flag = Flag::kGet;
  third_party::SArray<Key> keys{1};
  third_party::SArray<float> vals{0.1};
  msg.AddData(keys);
  msg.AddData(vals);

  // Push the firstbmsg
  send_queue->Push(msg);
  Message res;
  mailbox.WaitAndPop(&res);
  EXPECT_EQ(res.meta.sender, msg.meta.sender);

  // Push the second msg
  msg.meta.sender = 543;
  send_queue->Push(msg);
  mailbox.WaitAndPop(&res);
  EXPECT_EQ(res.meta.sender, msg.meta.sender);

  sender.Stop();
}

}  // namespace
}  // namespace flexps
