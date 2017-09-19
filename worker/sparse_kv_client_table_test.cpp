#include "gtest/gtest.h"
#include "glog/logging.h"

#include "worker/sparse_kv_client_table.hpp"
#include "worker/fake_callback_runner.hpp"

#include <condition_variable>
#include <mutex>
#include <thread>

namespace flexps {
namespace {

class TestSparseKVClientTable : public testing::Test {
 public:
  TestSparseKVClientTable() {}
  ~TestSparseKVClientTable() {}

 protected:
  void SetUp() {}
  void TearDown() {}
};

const uint32_t kTestAppThreadId = 15;
const uint32_t kTestModelId = 23;

TEST_F(TestSparseKVClientTable, Init) {
  ThreadsafeQueue<Message> queue;
  SimpleRangeManager manager({{2, 4}, {4, 7}, {7, 10}}, {0, 1, 2});
  FakeCallbackRunner callback_runner(kTestAppThreadId, kTestModelId);
  const int kSpeculation = 1;
  const std::vector<std::vector<Key>> keys{{2, 5}, {2, 5}};
  SparseKVClientTable<float> table(kTestAppThreadId, kTestModelId, &queue, &manager, &callback_runner,
      kSpeculation, keys);
}

TEST_F(TestSparseKVClientTable, Get) {
  ThreadsafeQueue<Message> queue;
  SimpleRangeManager manager({{2, 4}, {4, 7}, {7, 10}}, {0, 1, 2});
  FakeCallbackRunner callback_runner(kTestAppThreadId, kTestModelId);
  const int kSpeculation = 1;
  const std::vector<std::vector<Key>> keys{{2, 5}, {3, 6}};
  std::thread th([&queue, &manager, &callback_runner, kSpeculation, keys]() {
    SparseKVClientTable<float> table(kTestAppThreadId, kTestModelId, &queue, &manager, &callback_runner,
        kSpeculation, keys);
    std::vector<float> ret;
    table.Get(&ret);
    std::vector<float> expected{0.1, 0.2};
    EXPECT_EQ(ret, expected);
  });
  // Check the requests in queue
  Message msg;
  third_party::SArray<Key> res_keys;

  queue.WaitAndPop(&msg);
  EXPECT_EQ(msg.meta.sender, kTestAppThreadId);
  EXPECT_EQ(msg.meta.recver, 0);  // to 0
  EXPECT_EQ(msg.meta.model_id, kTestModelId);
  EXPECT_EQ(msg.meta.flag, Flag::kGet);
  EXPECT_EQ(msg.data.size(), 1);
  res_keys = msg.data[0];
  EXPECT_EQ(res_keys.size(), 1);
  EXPECT_EQ(res_keys[0], keys[0][0]);  // 2

  queue.WaitAndPop(&msg);
  EXPECT_EQ(msg.meta.sender, kTestAppThreadId);
  EXPECT_EQ(msg.meta.recver, 1);  // to 1
  EXPECT_EQ(msg.meta.model_id, kTestModelId);
  EXPECT_EQ(msg.meta.flag, Flag::kGet);
  EXPECT_EQ(msg.data.size(), 1);
  res_keys = msg.data[0];
  EXPECT_EQ(res_keys.size(), 1);
  EXPECT_EQ(res_keys[0], keys[0][1]);  // 5

  queue.WaitAndPop(&msg);
  EXPECT_EQ(msg.meta.sender, kTestAppThreadId);
  EXPECT_EQ(msg.meta.recver, 0);  // to 0
  EXPECT_EQ(msg.meta.model_id, kTestModelId);
  EXPECT_EQ(msg.meta.flag, Flag::kGet);
  EXPECT_EQ(msg.data.size(), 1);
  res_keys = msg.data[0];
  EXPECT_EQ(res_keys.size(), 1);
  EXPECT_EQ(res_keys[0], keys[1][0]);  // 3

  queue.WaitAndPop(&msg);
  EXPECT_EQ(msg.meta.sender, kTestAppThreadId);
  EXPECT_EQ(msg.meta.recver, 1);  // to 1
  EXPECT_EQ(msg.meta.model_id, kTestModelId);
  EXPECT_EQ(msg.meta.flag, Flag::kGet);
  EXPECT_EQ(msg.data.size(), 1);
  res_keys = msg.data[0];
  EXPECT_EQ(res_keys.size(), 1);
  EXPECT_EQ(res_keys[0], keys[1][1]);  // 6

  // AddResponse
  Message r1, r2;
  third_party::SArray<Key> r1_keys{2};
  third_party::SArray<float> r1_vals{0.1};
  r1.AddData(r1_keys);
  r1.AddData(r1_vals);
  third_party::SArray<Key> r2_keys{5};
  third_party::SArray<float> r2_vals{0.2};
  r2.AddData(r2_keys);
  r2.AddData(r2_vals);
  callback_runner.AddResponse(r1);
  callback_runner.AddResponse(r2);

  th.join();
}

TEST_F(TestSparseKVClientTable, GetAdd) {
  ThreadsafeQueue<Message> queue;
  SimpleRangeManager manager({{2, 4}, {4, 7}, {7, 10}}, {0, 1, 2});
  FakeCallbackRunner callback_runner(kTestAppThreadId, kTestModelId);
  const int kSpeculation = 1;
  const std::vector<std::vector<Key>> keys{{2, 5}, {3, 6}};
  std::thread th([&queue, &manager, &callback_runner, kSpeculation, keys]() {
    SparseKVClientTable<float> table(kTestAppThreadId, kTestModelId, &queue, &manager, &callback_runner,
        kSpeculation, keys);
    std::vector<float> ret;
    table.Get(&ret);
    std::vector<float> expected{0.1, 0.2};
    EXPECT_EQ(ret, expected);

    std::vector<float> vals{0.01, 0.02};
    table.Add(keys[0], vals);
  });
  // Check the requests in queue
  Message msg;
  third_party::SArray<Key> res_keys;

  // The Get requests for 1st iteration
  queue.WaitAndPop(&msg);
  EXPECT_EQ(msg.meta.sender, kTestAppThreadId);
  EXPECT_EQ(msg.meta.recver, 0);  // to 0
  EXPECT_EQ(msg.meta.model_id, kTestModelId);
  EXPECT_EQ(msg.meta.flag, Flag::kGet);
  EXPECT_EQ(msg.data.size(), 1);
  res_keys = msg.data[0];
  EXPECT_EQ(res_keys.size(), 1);
  EXPECT_EQ(res_keys[0], keys[0][0]);  // 2

  queue.WaitAndPop(&msg);
  EXPECT_EQ(msg.meta.sender, kTestAppThreadId);
  EXPECT_EQ(msg.meta.recver, 1);  // to 1
  EXPECT_EQ(msg.meta.model_id, kTestModelId);
  EXPECT_EQ(msg.meta.flag, Flag::kGet);
  EXPECT_EQ(msg.data.size(), 1);
  res_keys = msg.data[0];
  EXPECT_EQ(res_keys.size(), 1);
  EXPECT_EQ(res_keys[0], keys[0][1]);  // 5

  // The Get requests for 2nd iteration
  queue.WaitAndPop(&msg);
  EXPECT_EQ(msg.meta.sender, kTestAppThreadId);
  EXPECT_EQ(msg.meta.recver, 0);  // to 0
  EXPECT_EQ(msg.meta.model_id, kTestModelId);
  EXPECT_EQ(msg.meta.flag, Flag::kGet);
  EXPECT_EQ(msg.data.size(), 1);
  res_keys = msg.data[0];
  EXPECT_EQ(res_keys.size(), 1);
  EXPECT_EQ(res_keys[0], keys[1][0]);  // 3

  queue.WaitAndPop(&msg);
  EXPECT_EQ(msg.meta.sender, kTestAppThreadId);
  EXPECT_EQ(msg.meta.recver, 1);  // to 1
  EXPECT_EQ(msg.meta.model_id, kTestModelId);
  EXPECT_EQ(msg.meta.flag, Flag::kGet);
  EXPECT_EQ(msg.data.size(), 1);
  res_keys = msg.data[0];
  EXPECT_EQ(res_keys.size(), 1);
  EXPECT_EQ(res_keys[0], keys[1][1]);  // 6

  // AddResponse for the Get for the 1st iteration, so that Get() can unblock
  Message r1, r2;
  third_party::SArray<Key> r1_keys{2};
  third_party::SArray<float> r1_vals{0.1};
  r1.AddData(r1_keys);
  r1.AddData(r1_vals);
  third_party::SArray<Key> r2_keys{5};
  third_party::SArray<float> r2_vals{0.2};
  r2.AddData(r2_keys);
  r2.AddData(r2_vals);
  callback_runner.AddResponse(r1);
  callback_runner.AddResponse(r2);

  // The Add Requests for the 1st iteration
  third_party::SArray<float> res_vals;
  queue.WaitAndPop(&msg);
  EXPECT_EQ(msg.meta.sender, kTestAppThreadId);
  EXPECT_EQ(msg.meta.recver, 0);  // to 0
  EXPECT_EQ(msg.meta.model_id, kTestModelId);
  EXPECT_EQ(msg.meta.flag, Flag::kAdd);
  EXPECT_EQ(msg.data.size(), 2);
  res_keys = msg.data[0];
  res_vals = msg.data[1];
  EXPECT_EQ(res_keys.size(), 1);
  EXPECT_EQ(res_vals.size(), 1);
  EXPECT_EQ(res_keys[0], keys[0][0]);  // 2
  EXPECT_EQ(res_vals[0], float(0.01));

  queue.WaitAndPop(&msg);
  EXPECT_EQ(msg.meta.sender, kTestAppThreadId);
  EXPECT_EQ(msg.meta.recver, 1);  // to 1
  EXPECT_EQ(msg.meta.model_id, kTestModelId);
  EXPECT_EQ(msg.meta.flag, Flag::kAdd);
  EXPECT_EQ(msg.data.size(), 2);
  res_keys = msg.data[0];
  res_vals = msg.data[1];
  EXPECT_EQ(res_keys.size(), 1);
  EXPECT_EQ(res_vals.size(), 1);
  EXPECT_EQ(res_keys[0], keys[0][1]);  // 5
  EXPECT_EQ(res_vals[0], float(0.02));

  th.join();
}

TEST_F(TestSparseKVClientTable, GetAddGet) {
  ThreadsafeQueue<Message> queue;
  SimpleRangeManager manager({{2, 4}, {4, 7}, {7, 10}}, {0, 1, 2});
  FakeCallbackRunner callback_runner(kTestAppThreadId, kTestModelId);
  const int kSpeculation = 1;
  const std::vector<std::vector<Key>> keys{{2, 5}, {3, 6}, {7}};
  std::thread th([&queue, &manager, &callback_runner, kSpeculation, keys]() {
    SparseKVClientTable<float> table(kTestAppThreadId, kTestModelId, &queue, &manager, &callback_runner,
        kSpeculation, keys);
    std::vector<float> ret;
    table.Get(&ret);
    std::vector<float> expected{0.1, 0.2};
    EXPECT_EQ(ret, expected);

    std::vector<float> vals{0.01, 0.02};
    table.Add(keys[0], vals);

    table.Get(&ret);
    expected = {0.3, 0.4};
    EXPECT_EQ(ret, expected);
  });
  // Check the requests in queue
  Message msg;
  third_party::SArray<Key> res_keys;

  // The Get requests for 1st iteration
  queue.WaitAndPop(&msg);
  EXPECT_EQ(msg.meta.sender, kTestAppThreadId);
  EXPECT_EQ(msg.meta.recver, 0);  // to 0
  EXPECT_EQ(msg.meta.model_id, kTestModelId);
  EXPECT_EQ(msg.meta.flag, Flag::kGet);
  EXPECT_EQ(msg.data.size(), 1);
  res_keys = msg.data[0];
  EXPECT_EQ(res_keys.size(), 1);
  EXPECT_EQ(res_keys[0], keys[0][0]);  // 2

  queue.WaitAndPop(&msg);
  EXPECT_EQ(msg.meta.sender, kTestAppThreadId);
  EXPECT_EQ(msg.meta.recver, 1);  // to 1
  EXPECT_EQ(msg.meta.model_id, kTestModelId);
  EXPECT_EQ(msg.meta.flag, Flag::kGet);
  EXPECT_EQ(msg.data.size(), 1);
  res_keys = msg.data[0];
  EXPECT_EQ(res_keys.size(), 1);
  EXPECT_EQ(res_keys[0], keys[0][1]);  // 5

  // The Get requests for 2nd iteration
  queue.WaitAndPop(&msg);
  EXPECT_EQ(msg.meta.sender, kTestAppThreadId);
  EXPECT_EQ(msg.meta.recver, 0);  // to 0
  EXPECT_EQ(msg.meta.model_id, kTestModelId);
  EXPECT_EQ(msg.meta.flag, Flag::kGet);
  EXPECT_EQ(msg.data.size(), 1);
  res_keys = msg.data[0];
  EXPECT_EQ(res_keys.size(), 1);
  EXPECT_EQ(res_keys[0], keys[1][0]);  // 3

  queue.WaitAndPop(&msg);
  EXPECT_EQ(msg.meta.sender, kTestAppThreadId);
  EXPECT_EQ(msg.meta.recver, 1);  // to 1
  EXPECT_EQ(msg.meta.model_id, kTestModelId);
  EXPECT_EQ(msg.meta.flag, Flag::kGet);
  EXPECT_EQ(msg.data.size(), 1);
  res_keys = msg.data[0];
  EXPECT_EQ(res_keys.size(), 1);
  EXPECT_EQ(res_keys[0], keys[1][1]);  // 6

  // AddResponse for the Get requests for 1st iteration, so that Get() can unblock
  Message r1, r2;
  third_party::SArray<Key> r1_keys{2};
  third_party::SArray<float> r1_vals{0.1};
  r1.AddData(r1_keys);
  r1.AddData(r1_vals);
  third_party::SArray<Key> r2_keys{5};
  third_party::SArray<float> r2_vals{0.2};
  r2.AddData(r2_keys);
  r2.AddData(r2_vals);
  callback_runner.AddResponse(r1);
  callback_runner.AddResponse(r2);

  // The Add requests for the 1st iteration
  third_party::SArray<float> res_vals;
  queue.WaitAndPop(&msg);
  EXPECT_EQ(msg.meta.sender, kTestAppThreadId);
  EXPECT_EQ(msg.meta.recver, 0);  // to 0
  EXPECT_EQ(msg.meta.model_id, kTestModelId);
  EXPECT_EQ(msg.meta.flag, Flag::kAdd);
  EXPECT_EQ(msg.data.size(), 2);
  res_keys = msg.data[0];
  res_vals = msg.data[1];
  EXPECT_EQ(res_keys.size(), 1);
  EXPECT_EQ(res_vals.size(), 1);
  EXPECT_EQ(res_keys[0], keys[0][0]);  // 2
  EXPECT_EQ(res_vals[0], float(0.01));

  queue.WaitAndPop(&msg);
  EXPECT_EQ(msg.meta.sender, kTestAppThreadId);
  EXPECT_EQ(msg.meta.recver, 1);  // to 1
  EXPECT_EQ(msg.meta.model_id, kTestModelId);
  EXPECT_EQ(msg.meta.flag, Flag::kAdd);
  EXPECT_EQ(msg.data.size(), 2);
  res_keys = msg.data[0];
  res_vals = msg.data[1];
  EXPECT_EQ(res_keys.size(), 1);
  EXPECT_EQ(res_vals.size(), 1);
  EXPECT_EQ(res_keys[0], keys[0][1]);  // 5
  EXPECT_EQ(res_vals[0], float(0.02));

  // The Clock messages for the 1st iteration, sent before next Get
  queue.WaitAndPop(&msg);
  EXPECT_EQ(msg.meta.sender, kTestAppThreadId);
  EXPECT_EQ(msg.meta.recver, 0);  // to 0
  EXPECT_EQ(msg.meta.model_id, kTestModelId);
  EXPECT_EQ(msg.meta.flag, Flag::kClock);
  queue.WaitAndPop(&msg);
  EXPECT_EQ(msg.meta.sender, kTestAppThreadId);
  EXPECT_EQ(msg.meta.recver, 1);  // to 1
  EXPECT_EQ(msg.meta.model_id, kTestModelId);
  EXPECT_EQ(msg.meta.flag, Flag::kClock);
  queue.WaitAndPop(&msg);
  EXPECT_EQ(msg.meta.sender, kTestAppThreadId);
  EXPECT_EQ(msg.meta.recver, 2);  // to 2
  EXPECT_EQ(msg.meta.model_id, kTestModelId);
  EXPECT_EQ(msg.meta.flag, Flag::kClock);

  // The Get requests for the 3rd iteration
  queue.WaitAndPop(&msg);
  EXPECT_EQ(msg.meta.sender, kTestAppThreadId);
  EXPECT_EQ(msg.meta.recver, 2);  // to 2
  EXPECT_EQ(msg.meta.model_id, kTestModelId);
  EXPECT_EQ(msg.meta.flag, Flag::kGet);
  EXPECT_EQ(msg.data.size(), 1);
  res_keys = msg.data[0];
  EXPECT_EQ(res_keys.size(), 1);
  EXPECT_EQ(res_keys[0], keys[2][0]);  // 7

  // AddResponse for the Get requests for 2nd iteration, so that Get() can unblock
  Message r3, r4;
  third_party::SArray<Key> r3_keys{3};
  third_party::SArray<float> r3_vals{0.3};
  r3.AddData(r3_keys);
  r3.AddData(r3_vals);
  third_party::SArray<Key> r4_keys{6};
  third_party::SArray<float> r4_vals{0.4};
  r4.AddData(r4_keys);
  r4.AddData(r4_vals);
  callback_runner.AddResponse(r3);
  callback_runner.AddResponse(r4);

  th.join();
}

}  // namespace
}  // namespace flexps
