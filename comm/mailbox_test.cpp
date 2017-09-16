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

TEST_F(TestMailbox, ConnectAndBind) {
  Node node{0, "localhost", 32145};
  FakeIdMapper id_mapper;
  Mailbox mailbox(node, {node}, &id_mapper);
  mailbox.ConnectAndBind();
  mailbox.CloseSockets();
}

TEST_F(TestMailbox, StartStop) {
  Node node{0, "localhost", 32145};
  FakeIdMapper id_mapper;
  Mailbox mailbox(node, {node}, &id_mapper);
  mailbox.Start();
  mailbox.Stop();
}

TEST_F(TestMailbox, SendAndRecv) {
  Node node{0, "localhost", 32145};
  FakeIdMapper id_mapper;
  Mailbox mailbox(node, {node}, &id_mapper);
  mailbox.ConnectAndBind();

  Message msg;
  msg.meta.sender = 234;
  msg.meta.recver = 0;
  msg.meta.model_id = 45;
  msg.meta.flag = Flag::kGet;
  third_party::SArray<Key> keys{1};
  third_party::SArray<float> vals{0.4};
  msg.AddData(keys);
  msg.AddData(vals);

  mailbox.Send(msg);
  Message recv_msg;
  mailbox.Recv(&recv_msg);
  EXPECT_EQ(recv_msg.meta.sender, msg.meta.sender);
  EXPECT_EQ(recv_msg.meta.recver, msg.meta.recver);
  EXPECT_EQ(recv_msg.meta.model_id, msg.meta.model_id);
  EXPECT_EQ(recv_msg.meta.flag, msg.meta.flag);
  EXPECT_EQ(recv_msg.data.size(), 2);
  third_party::SArray<Key> recv_keys;
  recv_keys = recv_msg.data[0];
  third_party::SArray<float> recv_vals;
  recv_vals = recv_msg.data[1];
  EXPECT_EQ(recv_keys[0], keys[0]);
  EXPECT_EQ(recv_vals[0], vals[0]);

  mailbox.CloseSockets();
}
TEST_F(TestMailbox, Receiving) {
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
  third_party::SArray<Key> keys{1};
  third_party::SArray<float> vals{0.4};
  msg.AddData(keys);
  msg.AddData(vals);

  mailbox.Send(msg);
  Message recv_msg;
  queue.WaitAndPop(&recv_msg);
  EXPECT_EQ(recv_msg.meta.sender, msg.meta.sender);
  EXPECT_EQ(recv_msg.meta.recver, msg.meta.recver);
  EXPECT_EQ(recv_msg.meta.model_id, msg.meta.model_id);
  EXPECT_EQ(recv_msg.meta.flag, msg.meta.flag);
  EXPECT_EQ(recv_msg.data.size(), 2);
  third_party::SArray<Key> recv_keys;
  recv_keys = recv_msg.data[0];
  third_party::SArray<float> recv_vals;
  recv_vals = recv_msg.data[1];
  EXPECT_EQ(recv_keys[0], keys[0]);
  EXPECT_EQ(recv_vals[0], vals[0]);

  mailbox.Stop();
}

TEST_F(TestMailbox, SendRecvTwoNodes) {
  Node node1{0, "localhost", 32149};
  Node node2{1, "localhost", 32148};
  Message msg;
  msg.meta.sender = 234;
  msg.meta.recver = 1;
  msg.meta.model_id = 45;
  msg.meta.flag = Flag::kGet;
  third_party::SArray<Key> keys{1};
  third_party::SArray<float> vals{0.4};
  msg.AddData(keys);
  msg.AddData(vals);
  std::thread th1([=]() {
    FakeIdMapper id_mapper;
    Mailbox mailbox(node1, {node1, node2}, &id_mapper);
    mailbox.ConnectAndBind();
    mailbox.Send(msg);
    mailbox.CloseSockets();
  });
  std::thread th2([=]() {
    FakeIdMapper id_mapper;
    Mailbox mailbox(node2, {node1, node2}, &id_mapper);
    mailbox.ConnectAndBind();
    Message recv_msg;
    mailbox.Recv(&recv_msg);
    EXPECT_EQ(recv_msg.meta.sender, msg.meta.sender);
    EXPECT_EQ(recv_msg.meta.recver, msg.meta.recver);
    EXPECT_EQ(recv_msg.meta.model_id, msg.meta.model_id);
    EXPECT_EQ(recv_msg.meta.flag, msg.meta.flag);
    EXPECT_EQ(recv_msg.data.size(), 2);
    third_party::SArray<Key> recv_keys;
    recv_keys = recv_msg.data[0];
    third_party::SArray<float> recv_vals;
    recv_vals = recv_msg.data[1];
    EXPECT_EQ(recv_keys[0], keys[0]);
    EXPECT_EQ(recv_vals[0], vals[0]);
    mailbox.CloseSockets();
  });
  th1.join();
  th2.join();
}
TEST_F(TestMailbox, ReceivingTwoNodes) {
  Node node1{0, "localhost", 32149};
  Node node2{1, "localhost", 32148};
  Message msg;
  msg.meta.sender = 234;
  msg.meta.recver = 1;
  msg.meta.model_id = 45;
  msg.meta.flag = Flag::kGet;
  third_party::SArray<Key> keys{1};
  third_party::SArray<float> vals{0.4};
  msg.AddData(keys);
  msg.AddData(vals);
  std::thread th1([=]() {
    FakeIdMapper id_mapper;
    Mailbox mailbox(node1, {node1, node2}, &id_mapper);
    mailbox.Start();
    mailbox.Send(msg);
    mailbox.Stop();
  });
  std::thread th2([=]() {
    FakeIdMapper id_mapper;
    Mailbox mailbox(node2, {node1, node2}, &id_mapper);
    ThreadsafeQueue<Message> queue;
    mailbox.RegisterQueue(1, &queue);
    mailbox.Start();
    Message recv_msg;
    queue.WaitAndPop(&recv_msg);
    EXPECT_EQ(recv_msg.meta.sender, msg.meta.sender);
    EXPECT_EQ(recv_msg.meta.recver, msg.meta.recver);
    EXPECT_EQ(recv_msg.meta.model_id, msg.meta.model_id);
    EXPECT_EQ(recv_msg.meta.flag, msg.meta.flag);
    EXPECT_EQ(recv_msg.data.size(), 2);
    third_party::SArray<Key> recv_keys;
    recv_keys = recv_msg.data[0];
    third_party::SArray<float> recv_vals;
    recv_vals = recv_msg.data[1];
    EXPECT_EQ(recv_keys[0], keys[0]);
    EXPECT_EQ(recv_vals[0], vals[0]);
    mailbox.Stop();
  });
  th1.join();
  th2.join();
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
