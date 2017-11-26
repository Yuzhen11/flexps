#include "gtest/gtest.h"
#include "glog/logging.h"

#include "worker/kv_chunk_client_table.hpp"
#include "worker/fake_callback_runner.hpp"

#include <condition_variable>
#include <mutex>
#include <thread>

namespace flexps {
namespace{

class TestKVChunkClientTable : public testing::Test {
 public:
  TestKVChunkClientTable() {}
  ~TestKVChunkClientTable() {}

 protected:
  void SetUp() {}
  void TearDown() {}
};

const uint32_t kTestAppThreadId = 15;
const uint32_t kTestModelId = 23;

TEST_F(TestKVChunkClientTable, Init) {
  ThreadsafeQueue<Message> queue;
  SimpleRangePartitionManager manager({{2, 4}, {4, 7}}, {0, 1});
  FakeCallbackRunner callback_runner(kTestAppThreadId, kTestModelId);
  KVChunkClientTable<float> table(kTestAppThreadId, kTestModelId, &queue, &manager, &callback_runner);
}


TEST_F(TestKVChunkClientTable, VectorChunkAdd) {
  ThreadsafeQueue<Message> queue;
  SimpleRangePartitionManager manager({{4, 8}, {8, 14}}, {0, 1});
  FakeCallbackRunner callback_runner(kTestAppThreadId, kTestModelId);
  KVChunkClientTable<float> table(kTestAppThreadId, kTestModelId, &queue, &manager, &callback_runner);
  std::vector<Key> keys = {3, 4, 5, 6};
  std::vector<std::vector<float>> vals = {{0.1, 0.1}, {0.2, 0.2}, {0.3, 0.3}, {0.4, 0.4}};
  table.AddChunk(keys, vals);  // {3,4,5,6} -> {3}, {4,5,6}
  Message m1, m2;
  queue.WaitAndPop(&m1);
  queue.WaitAndPop(&m2);

  third_party::SArray<Key> res_keys;
  third_party::SArray<float> res_vals;
  EXPECT_EQ(m1.meta.sender, kTestAppThreadId);
  EXPECT_EQ(m1.meta.recver, 0);
  EXPECT_EQ(m1.meta.model_id, kTestModelId);
  EXPECT_EQ(m1.meta.flag, Flag::kAdd);
  ASSERT_EQ(m1.data.size(), 2);
  res_keys = m1.data[0];
  res_vals = m1.data[1];
  ASSERT_EQ(res_keys.size(), 2);
  ASSERT_EQ(res_vals.size(), 2);
  EXPECT_EQ(res_keys[0], 6);
  EXPECT_EQ(res_keys[1], 7);
  EXPECT_EQ(res_vals[0], float(0.1));
  EXPECT_EQ(res_vals[1], float(0.1));

  EXPECT_EQ(m2.meta.sender, kTestAppThreadId);
  EXPECT_EQ(m2.meta.recver, 1);
  EXPECT_EQ(m2.meta.model_id, kTestModelId);
  EXPECT_EQ(m2.meta.flag, Flag::kAdd);
  ASSERT_EQ(m2.data.size(), 2);
  res_keys = m2.data[0];
  res_vals = m2.data[1];
  ASSERT_EQ(res_keys.size(), 6);
  EXPECT_EQ(res_vals.size(), 6);
  EXPECT_EQ(res_keys[0], 8);
  EXPECT_EQ(res_keys[1], 9);
  EXPECT_EQ(res_keys[2], 10);
  EXPECT_EQ(res_keys[3], 11);
  EXPECT_EQ(res_keys[4], 12);
  EXPECT_EQ(res_keys[5], 13);
  EXPECT_EQ(res_vals[0], float(0.2));
  EXPECT_EQ(res_vals[1], float(0.2));
  EXPECT_EQ(res_vals[2], float(0.3));
  EXPECT_EQ(res_vals[3], float(0.3));
  EXPECT_EQ(res_vals[4], float(0.4));
  EXPECT_EQ(res_vals[5], float(0.4));
}

TEST_F(TestKVChunkClientTable, VectorChunkGet) {
  ThreadsafeQueue<Message> queue;
  SimpleRangePartitionManager manager({{4, 8}, {8, 14}}, {0, 1});
  FakeCallbackRunner callback_runner(kTestAppThreadId, kTestModelId);
  std::thread th([&queue, &manager, &callback_runner]() {
    KVChunkClientTable<float> table(kTestAppThreadId, kTestModelId, &queue, &manager, &callback_runner);
    std::vector<Key> keys = {3, 4, 5, 6};
    std::vector<std::vector<float>*> vals(4);
    for (int i = 0; i < 4; i++)
      vals[i] = new std::vector<float>(2);
    table.GetChunk(keys, vals);  // {3,4,5,6} -> {3}, {4,5,6}
    std::vector<std::vector<float>> expected{{0.1, 0.1}, {0.2, 0.2}, {0.3, 0.3}, {0.4, 0.4}};
    EXPECT_EQ(*(vals[0]), expected[0]);
    EXPECT_EQ(*(vals[1]), expected[1]);
    EXPECT_EQ(*(vals[2]), expected[2]);
    EXPECT_EQ(*(vals[3]), expected[3]);
  });
  // Check the requests in queue
  Message m1, m2;
  queue.WaitAndPop(&m1);
  queue.WaitAndPop(&m2);

  EXPECT_EQ(m1.meta.sender, kTestAppThreadId);
  EXPECT_EQ(m1.meta.recver, 0);
  EXPECT_EQ(m1.meta.model_id, kTestModelId);
  EXPECT_EQ(m1.meta.flag, Flag::kGet);
  ASSERT_EQ(m1.data.size(), 1);
  third_party::SArray<Key> res_keys;
  res_keys = m1.data[0];
  ASSERT_EQ(res_keys.size(), 2);
  EXPECT_EQ(res_keys[0], 6);
  EXPECT_EQ(res_keys[1], 7);
  EXPECT_EQ(m2.meta.sender, kTestAppThreadId);
  EXPECT_EQ(m2.meta.recver, 1);
  EXPECT_EQ(m2.meta.model_id, kTestModelId);
  EXPECT_EQ(m2.meta.flag, Flag::kGet);
  ASSERT_EQ(m2.data.size(), 1);
  res_keys = m2.data[0];
  ASSERT_EQ(res_keys.size(), 6);
  EXPECT_EQ(res_keys[0], 8);
  EXPECT_EQ(res_keys[1], 9);
  EXPECT_EQ(res_keys[2], 10);
  EXPECT_EQ(res_keys[3], 11);
  EXPECT_EQ(res_keys[4], 12);
  EXPECT_EQ(res_keys[5], 13);

  // AddResponse
  Message r1, r2, r3;
  third_party::SArray<Key> r1_keys{6, 7};
  third_party::SArray<float> r1_vals{0.1, 0.1};
  r1.AddData(r1_keys);
  r1.AddData(r1_vals);
  third_party::SArray<Key> r2_keys{8, 9, 10, 11, 12, 13};
  third_party::SArray<float> r2_vals{0.2, 0.2, 0.3, 0.3, 0.4, 0.4};
  r2.AddData(r2_keys);
  r2.AddData(r2_vals);
  callback_runner.AddResponse(r1);
  callback_runner.AddResponse(r2);
  th.join();
}

}  // namespace
}  // namespace flexps
