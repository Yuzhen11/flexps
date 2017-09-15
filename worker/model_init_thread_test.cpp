#include "gtest/gtest.h"
#include "glog/logging.h"

#include "model_init_thread.hpp"

namespace flexps {
namespace {

class TestModelInitThread : public testing::Test {
 public:
  TestModelInitThread() {}
  ~TestModelInitThread() {}

 protected:
  void SetUp() {}
  void TearDown() {}
};

TEST_F(TestModelInitThread, StartStop) {
  ThreadsafeQueue<Message> downstream;
  ModelInitThread model_init_thread(0, &downstream);
  auto* work_queue = model_init_thread.GetWorkQueue();
  model_init_thread.Start();

  Message exit_msg;
  exit_msg.meta.flag = Flag::kExit;
  work_queue->Push(exit_msg);
  model_init_thread.Stop();
}

TEST_F(TestModelInitThread, ResetWorkerInModel) {
  ThreadsafeQueue<Message> downstream;
  ModelInitThread model_init_thread(0, &downstream);
  auto* work_queue = model_init_thread.GetWorkQueue();
  model_init_thread.Start();

  const uint32_t kTestModelId = 32;
  const std::vector<uint32_t> kTestLocalServers = {542};
  const std::vector<uint32_t> kTestWorkerIds= {23, 43};


  std::thread th([&downstream, kTestModelId, kTestLocalServers, kTestWorkerIds, work_queue] {
    Message msg;
    downstream.WaitAndPop(&msg);
    EXPECT_EQ(msg.meta.recver, kTestLocalServers[0]);
    EXPECT_EQ(msg.meta.model_id, kTestModelId);
    EXPECT_EQ(msg.meta.flag, Flag::kResetWorkerInModel);
    EXPECT_EQ(msg.data.size(), 1);
    third_party::SArray<uint32_t> wids;
    wids = msg.data[0];
    EXPECT_EQ(wids.size(), 2);
    EXPECT_EQ(wids[0], kTestWorkerIds[0]);
    EXPECT_EQ(wids[1], kTestWorkerIds[1]);
    // Act as the reply from local server
    work_queue->Push(msg);
  });

  model_init_thread.ResetWorkerInModel(kTestModelId, kTestLocalServers, kTestWorkerIds);

  th.join();

  Message exit_msg;
  exit_msg.meta.flag = Flag::kExit;
  work_queue->Push(exit_msg);
  model_init_thread.Stop();
}

}  // namespace
}  // namespace flexps
