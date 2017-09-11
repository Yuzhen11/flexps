#include "gtest/gtest.h"

#include "glog/logging.h"

#include "worker/abstract_receiver.hpp"
#include "worker/worker_helper_thread.hpp"

namespace flexps {
namespace {

class FakeReceiver : public AbstractReceiver {
 public:
  virtual void AddResponse(uint32_t app_thread_id, uint32_t model_id, Message& msg) override {
    EXPECT_EQ(app_thread_id, expected_app_thread_id_);
    EXPECT_EQ(model_id, expected_model_id_);
  }
  void SetExpected(uint32_t app_thread_id, uint32_t model_id) {
    expected_app_thread_id_ = app_thread_id;
    expected_model_id_ = model_id;
  }
 private:
  uint32_t expected_app_thread_id_;
  uint32_t expected_model_id_;
};

class TestWorkerHelperThread : public testing::Test {
 public:
  TestWorkerHelperThread() {}
  ~TestWorkerHelperThread() {}

 protected:
  void SetUp() {}
  void TearDown() {}
};

TEST_F(TestWorkerHelperThread, Init) {
  FakeReceiver receiver;
  WorkerHelperThread helper(0, &receiver);
}

TEST_F(TestWorkerHelperThread, StartStop) {
  FakeReceiver receiver;
  WorkerHelperThread helper_thread(0, &receiver);
  auto* work_queue = helper_thread.GetWorkQueue();
  helper_thread.Start();
  Message exit_msg;
  exit_msg.meta.flag = Flag::kExit; 
  work_queue->Push(exit_msg);
  helper_thread.Stop();
}

TEST_F(TestWorkerHelperThread, AddResponse) {
  FakeReceiver receiver;
  WorkerHelperThread helper_thread(0, &receiver);
  auto* work_queue = helper_thread.GetWorkQueue();
  helper_thread.Start();

  const uint32_t kRecver = 123;
  const uint32_t kModelId = 87;
  Message msg1;
  msg1.meta.recver = kRecver;
  msg1.meta.model_id = kModelId;
  receiver.SetExpected(kRecver, kModelId);
  work_queue->Push(msg1);

  Message exit_msg;
  exit_msg.meta.flag = Flag::kExit; 
  work_queue->Push(exit_msg);
  helper_thread.Stop();
}

}  // namespace
}  // namespace flexps
