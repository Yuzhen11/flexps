#include "gtest/gtest.h"

#include "glog/logging.h"

#include "worker/kv_client_table.hpp"

#include <thread>
#include <condition_variable>
#include <mutex>

namespace flexps {
namespace {

class TestKVClientTable : public testing::Test {
 public:
  TestKVClientTable() {}
  ~TestKVClientTable() {}

 protected:
  void SetUp() {}
  void TearDown() {}
};

class FakeRangeManager : public AbstractRangeManager {
 public:
  FakeRangeManager(const std::vector<third_party::Range>& ranges)
    : ranges_(ranges) {}
  virtual size_t GetNumServers() const override {
    return ranges_.size();
  }
  virtual const std::vector<third_party::Range>& GetRanges() const override {
    return ranges_;
  }
 private:
  std::vector<third_party::Range> ranges_;
};

const uint32_t kTestAppThreadId = 15;
const uint32_t kTestModelId = 23;

class FakeCallbackRunner : public AbstractCallbackRunner {
 public:
  FakeCallbackRunner() {}
  virtual void RegisterRecvHandle(uint32_t app_thread_id, uint32_t model_id, const std::function<void(Message&)>& recv_handle) override {
    EXPECT_EQ(app_thread_id, kTestAppThreadId);
    EXPECT_EQ(model_id, kTestModelId);
    recv_handle_ = recv_handle;
  }
  virtual void RegisterRecvFinishHandle(uint32_t app_thread_id, uint32_t model_id, const std::function<void()>& recv_finish_handle) override {
    EXPECT_EQ(app_thread_id, kTestAppThreadId);
    EXPECT_EQ(model_id, kTestModelId);
    recv_finish_handle_= recv_finish_handle;
  }

  virtual void NewRequest(uint32_t app_thread_id, uint32_t model_id, uint32_t expected_responses) override {
    EXPECT_EQ(app_thread_id, kTestAppThreadId);
    EXPECT_EQ(model_id, kTestModelId);
    tracker_ = {expected_responses, 0};
  }
  virtual void WaitRequest(uint32_t app_thread_id, uint32_t model_id) override {
    EXPECT_EQ(app_thread_id, kTestAppThreadId);
    EXPECT_EQ(model_id, kTestModelId);
    std::unique_lock<std::mutex> lk(mu_);
    cond_.wait(lk, [this] {
      return tracker_.first == tracker_.second;
    });
  }
  void AddResponse(Message m) {
    EXPECT_NE(recv_handle_, nullptr);
    bool recv_finish = false;
    {
      std::lock_guard<std::mutex> lk(mu_);
      recv_finish = tracker_.first == tracker_.second + 1 ?  true : false;
    }
    recv_handle_(m);
    if (recv_finish) {
      recv_finish_handle_();
    }
    {
      std::lock_guard<std::mutex> lk(mu_);
      tracker_.second += 1;
      if (recv_finish) {
        cond_.notify_all();
      }
    }
  }
 private:
  std::function<void(Message&)> recv_handle_;
  std::function<void()> recv_finish_handle_;

  std::mutex mu_;
  std::condition_variable cond_;
  std::pair<uint32_t, uint32_t> tracker_;
};

TEST_F(TestKVClientTable, Init) {
  ThreadsafeQueue<Message> queue;
  FakeRangeManager manager({{2, 4}, {4, 7}});
  FakeCallbackRunner callback_runner;
  KVClientTable<float> table(kTestAppThreadId, kTestModelId, &queue, &manager, &callback_runner);
}

TEST_F(TestKVClientTable, Add) {
  ThreadsafeQueue<Message> queue;
  FakeRangeManager manager({{2, 4}, {4, 7}});
  FakeCallbackRunner callback_runner;
  KVClientTable<float> table(kTestAppThreadId, kTestModelId, &queue, &manager, &callback_runner);
  std::vector<Key> keys = {3, 4, 5, 6};
  std::vector<float> vals = {0.1, 0.1, 0.1, 0.1};
  table.Add(keys, vals);  // {3,4,5,6} -> {3}, {4,5,6}
  Message m1, m2;
  queue.WaitAndPop(&m1);
  queue.WaitAndPop(&m2);
  EXPECT_EQ(m1.data.size(), 2);
  third_party::SArray<Key> res_keys;
  res_keys = m1.data[0];
  EXPECT_EQ(res_keys.size(), 1);
  third_party::SArray<float> res_vals;
  res_vals = m1.data[1];
  EXPECT_EQ(res_vals.size(), 1);
  EXPECT_EQ(res_keys[0], 3);
  EXPECT_EQ(res_vals[0], float(0.1));

  EXPECT_EQ(m2.data.size(), 2);
  res_keys = m2.data[0];
  EXPECT_EQ(res_keys.size(), 3);
  res_vals = m2.data[1];
  EXPECT_EQ(res_vals.size(), 3);
  EXPECT_EQ(res_keys[0], 4);
  EXPECT_EQ(res_keys[1], 5);
  EXPECT_EQ(res_keys[2], 6);
  EXPECT_EQ(res_vals[0], float(0.1));
  EXPECT_EQ(res_vals[1], float(0.1));
  EXPECT_EQ(res_vals[2], float(0.1));
}

TEST_F(TestKVClientTable, Get) {
  ThreadsafeQueue<Message> queue;
  FakeRangeManager manager({{2, 4}, {4, 7}});
  FakeCallbackRunner callback_runner;
  std::thread th([&queue, &manager, &callback_runner]() {
    KVClientTable<float> table(kTestAppThreadId, kTestModelId, &queue, &manager, &callback_runner);
    std::vector<Key> keys = {3, 4, 5, 6};
    std::vector<float> vals;
    table.Get(keys, &vals);  // {3,4,5,6} -> {3}, {4,5,6}
    std::vector<float> expected{0.1, 0.4, 0.2, 0.3};
    EXPECT_EQ(vals, expected);
  });
  // Check the requests in queue
  Message m1, m2;
  queue.WaitAndPop(&m1);
  queue.WaitAndPop(&m2);
  EXPECT_EQ(m1.data.size(), 2);
  third_party::SArray<Key> res_keys;
  res_keys = m1.data[0];
  EXPECT_EQ(res_keys.size(), 1);
  third_party::SArray<float> res_vals;
  res_vals = m1.data[1];
  EXPECT_EQ(res_vals.size(), 0);
  EXPECT_EQ(res_keys[0], 3);

  EXPECT_EQ(m2.data.size(), 2);
  res_keys = m2.data[0];
  EXPECT_EQ(res_keys.size(), 3);
  res_vals = m2.data[1];
  EXPECT_EQ(res_vals.size(), 0);
  EXPECT_EQ(res_keys[0], 4);
  EXPECT_EQ(res_keys[1], 5);
  EXPECT_EQ(res_keys[2], 6);

  // AddResponse
  Message r1, r2, r3;
  third_party::SArray<Key> r1_keys{3};
  third_party::SArray<float> r1_vals{0.1};
  r1.AddData(r1_keys);
  r1.AddData(r1_vals);
  third_party::SArray<Key> r2_keys{4,5,6};
  third_party::SArray<float> r2_vals{0.4, 0.2, 0.3};
  r2.AddData(r2_keys);
  r2.AddData(r2_vals);
  callback_runner.AddResponse(r1);
  callback_runner.AddResponse(r2);
  th.join();
}

}  // namespace
}  // namespace flexps
