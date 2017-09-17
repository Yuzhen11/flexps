#include "glog/logging.h"
#include "gtest/gtest.h"

#include "server/pending_buffer.hpp"

namespace flexps {
namespace {

class TestPendingBuffer : public testing::Test {
 public:
  TestPendingBuffer() {}
  ~TestPendingBuffer() {}

 protected:
  void SetUp() {}
  void TearDown() {}
};

TEST_F(TestPendingBuffer, Construct) { PendingBuffer pending_buffer; }

TEST_F(TestPendingBuffer, PushAndPop) {
  PendingBuffer pending_buffer;

  // Message m1
  Message m1;
  m1.meta.flag = Flag::kAdd;
  m1.meta.model_id = 0;
  m1.meta.sender = 2;
  m1.meta.recver = 0;
  third_party::SArray<int> m1_keys({0});
  third_party::SArray<int> m1_vals({1});
  m1.AddData(m1_keys);
  m1.AddData(m1_vals);

  // Message m2
  Message m2;
  m2.meta.flag = Flag::kAdd;
  m2.meta.model_id = 0;
  m2.meta.sender = 2;
  m2.meta.recver = 0;
  third_party::SArray<int> m2_keys({0});
  third_party::SArray<int> m2_vals({1});
  m2.AddData(m1_keys);
  m2.AddData(m1_vals);

  pending_buffer.Push(0, m1);
  pending_buffer.Push(0, m1);
  pending_buffer.Push(1, m2);

  EXPECT_EQ(pending_buffer.Size(0), 2);
  EXPECT_EQ(pending_buffer.Size(1), 1);

  std::vector<Message> messages_0 = pending_buffer.Pop(0);
  std::vector<Message> messages_1 = pending_buffer.Pop(1);

  EXPECT_EQ(messages_0.size(0), 2);
  EXPECT_EQ(messages_1.size(1), 1);
}

}  // namespace
}  // namespace flexps