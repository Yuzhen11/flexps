#include "glog/logging.h"
#include "gtest/gtest.h"

#include "server/sparse_pending_buffer.hpp"

namespace flexps {
namespace {

class TestSparsePendingBuffer : public testing::Test {
 public:
  TestSparsePendingBuffer() {}
  ~TestSparsePendingBuffer() {}

 protected:
  void SetUp() {}
  void TearDown() {}
};

TEST_F(TestSparsePendingBuffer, Construct) { SparsePendingBuffer pending_buffer; }

TEST_F(TestSparsePendingBuffer, PushAndPop) {
  SparsePendingBuffer pending_buffer;

  // Message m1
  Message m1;
  m1.meta.flag = Flag::kAdd;
  m1.meta.model_id = 0;
  m1.meta.sender = 1;
  m1.meta.recver = 0;
  m1.meta.version = 0;
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
  m2.meta.version = 0;
  third_party::SArray<int> m2_keys({0});
  third_party::SArray<int> m2_vals({1});
  m2.AddData(m1_keys);
  m2.AddData(m1_vals);

  // Message m3
  Message m3;
  m3.meta.flag = Flag::kAdd;
  m3.meta.model_id = 0;
  m3.meta.sender = 2;
  m3.meta.recver = 0;
  m3.meta.version = 1;
  third_party::SArray<int> m3_keys({0});
  third_party::SArray<int> m3_vals({1});
  m3.AddData(m3_keys);
  m3.AddData(m3_vals);

  pending_buffer.Push(0, m1, 1);
  pending_buffer.Push(0, m2, 2);
  pending_buffer.Push(1, m3, 2);

  EXPECT_EQ(pending_buffer.Size(0), 2);
  EXPECT_EQ(pending_buffer.Size(1), 1);

  std::vector<Message> messages_0 = pending_buffer.Pop(0, 1);
  std::vector<Message> messages_1 = pending_buffer.Pop(0, 2);
  std::vector<Message> messages_2 = pending_buffer.Pop(1, 2);

  EXPECT_EQ(messages_0.size(), 1);
  EXPECT_EQ(messages_1.size(), 1);
  EXPECT_EQ(messages_2.size(), 1);
}

}  // namespace
}  // namespace flexps