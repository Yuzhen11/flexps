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
  Mailbox mailbox(node);
  mailbox.Start({node});

  Message msg, msg2;
  msg.meta.sender = 0;
  msg.meta.recver = 0;
  msg.meta.model_id = 0;
  msg.meta.flag = Flag::kGet;

  third_party::SArray<Key> keys{4,5,6};
  third_party::SArray<float> vals{0.4, 0.2, 0.3};
  msg.AddData(keys);
  msg.AddData(vals);

  mailbox.Send(msg);

  msg2.meta.sender = 0;
  msg2.meta.recver = 0;
  msg.meta.model_id = 0;
  msg.meta.flag = Flag::kExit;
  mailbox.Send(msg2);
  
  mailbox.Stop();
}

}  // namespace
}  // namespace flexps
