#include "gtest/gtest.h"

#include "glog/logging.h"

#include "mailbox.hpp"

namespace flexps {
namespace {

class TestMailbox : public testing::Test {
 public:
  TestMailbox() {}
  ~TestMailbox() {}

 protected:
  void SetUp() {}
  void TearDown() {}
};

class FakeIdMapper : public AbstractIdMapper {
 public:
  virtual uint32_t GetNodeIdForThread(uint32_t tid) override {
    return tid;
  }
};

TEST_F(TestMailbox, StartStop) {
  Node node{0, "localhost", 32145};
  FakeIdMapper id_mapper;
  Mailbox mailbox(node, {node}, &id_mapper);
  ThreadsafeQueue<Message> queue;
  mailbox.RegisterQueue(0, &queue);
  mailbox.Start();
  mailbox.Stop();
}

TEST_F(TestMailbox, SendAndRecv) {

  Node node{0, "localhost", 32145};
  FakeIdMapper id_mapper;
  Mailbox mailbox(node, {node}, &id_mapper);
  ThreadsafeQueue<Message> queue;
  mailbox.RegisterQueue(0, &queue);
  mailbox.Start();

  Message msg;
  msg.meta.sender = 0;
  msg.meta.recver = 0;
  msg.meta.model_id = 0;
  msg.meta.flag = Flag::kGet;

  third_party::SArray<Key> keys{4, 5, 6};
  third_party::SArray<float> vals{0.4, 0.2, 0.3};
  msg.AddData(keys);
  msg.AddData(vals);

  mailbox.Send(msg);
  Message in_queue_msg;
  //mailbox.Recv(&in_queue_msg);
  queue.WaitAndPop(&in_queue_msg);
  EXPECT_EQ(in_queue_msg.meta.sender, msg.meta.sender);

  mailbox.Stop();
}

TEST_F(TestMailbox, BarrierTwoNodes) {
  Node node1{0, "localhost", 32149};
  Node node2{1, "localhost", 32148};
  std::thread th1([&]() {
    FakeIdMapper id_mapper;
    Mailbox mailbox(node1, {node1, node2}, &id_mapper);
    mailbox.Start();
    for (int i = 0; i < 10; ++ i) {
      mailbox.Barrier();
      LOG(INFO) << "iter " << i;
    }
    mailbox.Stop();
  });
  std::thread th2([&]() {
    FakeIdMapper id_mapper;
    Mailbox mailbox(node2, {node1, node2}, &id_mapper);
    mailbox.Start();
    for (int i = 0; i < 10; ++ i) {
      mailbox.Barrier();
      LOG(INFO) << "iter " << i;
    }
    mailbox.Stop();
  });
  th1.join();
  th2.join();
}

TEST_F(TestMailbox, BarrierFourNodes) {
  std::vector<Node> nodes{
    {0, "localhost", 43551},
    {1, "localhost", 43552},
    {2, "localhost", 43553},
    {3, "localhost", 43554}};

  std::vector<std::thread> threads(nodes.size());
  for (int i = 0; i < nodes.size(); ++ i) {
    LOG(INFO) << "i: " << i << "/" << nodes.size();
    threads[i] = std::thread([&nodes, i]() {
      LOG(INFO) << "running";
      FakeIdMapper id_mapper;
      Mailbox mailbox(nodes[i], nodes, &id_mapper);
      mailbox.Start();
      for (int j = 0; j < 10; ++ j) {
        mailbox.Barrier();
        LOG(INFO) << "iter " << j;
      }
      mailbox.Stop();
    });
  }
  for (auto& th : threads) {
    th.join();
  }
}

}  // namespace
}  // namespace flexps
