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

TEST_F(TestMailbox, Construct) {
  Node node{0, "localhost", 32145};
  Mailbox mailbox(node, {node});
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

  mailbox.Stop();
}

}  // namespace
}  // namespace flexps
