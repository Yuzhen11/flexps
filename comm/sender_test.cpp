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
    to_send.push_back(msg);
    return -1;
  }
  virtual int Recv(Message* msg) override { return -1; };
  virtual void Start() override{};
  virtual void Stop() override{};
  int CheckSize() { return to_send.size(); }

 private:
  virtual void Connect(const Node& node) override{};
  virtual void Bind(const Node& node) override{};

  virtual void Receiving() override{};

  std::vector<Message> to_send;
};

TEST_F(TestSender, Start) {
  FakeMailbox mailbox;
  mailbox.Start();

  Sender sender(&mailbox);
  auto message_queue = sender.GetMessageQueue();

  // Msg
  Message msg;
  msg.meta.sender = 0;
  msg.meta.recver = 0;
  msg.meta.model_id = 0;
  msg.meta.flag = Flag::kGet;

  third_party::SArray<Key> keys{4, 5, 6};
  third_party::SArray<float> vals{0.4, 0.2, 0.3};
  msg.AddData(keys);
  msg.AddData(vals);

  sender.Start();

  message_queue->Push(msg);

  // Wait some time
  std::this_thread::sleep_for(std::chrono::nanoseconds(10000));

  EXPECT_EQ(mailbox.CheckSize(), 1);

  message_queue->Push(msg);

  // Wait some time
  std::this_thread::sleep_for(std::chrono::nanoseconds(10000));

  EXPECT_EQ(mailbox.CheckSize(), 2);

  sender.Stop();
  mailbox.Stop();
}

}  // namespace
}  // namespace flexps
