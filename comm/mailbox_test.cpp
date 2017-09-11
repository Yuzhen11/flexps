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

  Message msg;
  msg.meta.sender = 0;
  msg.meta.recver = 0;
  msg.meta.model_id = 0;
  msg.meta.flag = Flag::kGet;

  mailbox.Send(msg);

  mailbox.Stop();
}

}  // namespace
}  // namespace flexps
