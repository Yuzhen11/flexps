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

// TODO(Jasper): Test for Send and Recv

TEST_F(TestMailbox, Send) {
  // TODO(Jasper): We cannot reuse the same port, maybe it's because we did not destroy the zmq context?
  // Please check and add zmq_ctx_destroy in Mailbox::Stop()
  Node node{0, "localhost", 32142};
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
  // TODO(Jasper): Check the message in queue

  mailbox.Stop();
}

}  // namespace
}  // namespace flexps
