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
  // Node node{0, "localhost", 32145};
  // Mailbox mailbox(node);
  // mailbox.Start({node});
  // mailbox.Stop();
}

}  // namespace
}  // namespace flexps
