#include "glog/logging.h"
#include "gtest/gtest.h"

#include "base/threadsafe_queue.hpp"
#include "server/bsp_model.hpp"

namespace flexps {
namespace {

class TestBSPModel : public testing::Test {
 public:
  TestBSPModel() {}
  ~TestBSPModel() {}

 protected:
  void SetUp() {}
  void TearDown() {}
};

TEST_F(TestBSPModel, CheckConstructor) {
  ThreadsafeQueue<Message> reply_queue;
  int model_id = 0;
  std::unique_ptr<AbstractStorage> storage(new Storage<int>());
  std::unique_ptr<AbstractModel> model(new BSPModel(model_id, std::move(storage), &reply_queue));
}

TEST_F(TestBSPModel, CheckGetAndAdd) {
  ThreadsafeQueue<Message> reply_queue;
  std::vector<uint32_t> tids{2, 3};
  int model_id = 0;
  std::unique_ptr<AbstractStorage> storage(new Storage<int>());
  std::unique_ptr<AbstractModel> model(new BSPModel(model_id, std::move(storage), &reply_queue));
  model->ResetWorker(tids);

  // Message0
  Message m0;
  m0.meta.flag = Flag::kGet;
  m0.meta.model_id = 0;
  m0.meta.sender = 2;
  m0.meta.recver = 0;
  third_party::SArray<int> m0_keys({1});
  m0.AddData(m0_keys);
  model->Get(m0);

  EXPECT_EQ(reply_queue.Size(), 1);
  Message useless0;
  reply_queue.WaitAndPop(&useless0);

  // Message1
  Message m1;
  m1.meta.flag = Flag::kAdd;
  m1.meta.model_id = 0;
  m1.meta.sender = 2;
  m1.meta.recver = 0;
  third_party::SArray<int> m1_keys({1});
  third_party::SArray<int> m1_vals({100});
  m1.AddData(m1_keys);
  m1.AddData(m1_vals);
  model->Add(m1);

  // Message2
  Message m2;
  m2.meta.flag = Flag::kClock;
  m2.meta.model_id = 0;
  m2.meta.sender = 2;
  m2.meta.recver = 0;
  model->Clock(m2);

  EXPECT_EQ(model->GetProgress(2), 1);
  EXPECT_EQ(model->GetProgress(3), 0);
  EXPECT_EQ(dynamic_cast<BSPModel*>(model.get())->GetAddPendingSize(), 1);

  // Check Message 1
  Message cm1;
  cm1.meta.flag = Flag::kGet;
  cm1.meta.model_id = 0;
  cm1.meta.sender = 3;
  cm1.meta.recver = 0;
  third_party::SArray<int> cm1_keys({1});
  cm1.AddData(cm1_keys);
  model->Get(cm1);

  Message check_message;
  EXPECT_EQ(reply_queue.Size(), 1);
  reply_queue.WaitAndPop(&check_message);
  auto rep_keys = third_party::SArray<int>(check_message.data[0]);
  auto rep_vals = third_party::SArray<int>(check_message.data[1]);
  EXPECT_EQ(rep_keys.size(), 1);
  EXPECT_EQ(rep_vals.size(), 1);
  EXPECT_EQ(rep_vals[0], 0);

  // Message3
  Message m3;
  m3.meta.flag = Flag::kClock;
  m3.meta.model_id = 0;
  m3.meta.sender = 3;
  m3.meta.recver = 0;
  model->Clock(m3);

  EXPECT_EQ(model->GetProgress(2), 1);
  EXPECT_EQ(model->GetProgress(3), 1);
  EXPECT_EQ(dynamic_cast<BSPModel*>(model.get())->GetAddPendingSize(), 0);

  // Check Message 2
  Message cm2;
  cm2.meta.flag = Flag::kGet;
  cm2.meta.model_id = 0;
  cm2.meta.sender = 3;
  cm2.meta.recver = 0;
  third_party::SArray<int> cm2_keys({1});
  cm2.AddData(cm2_keys);
  model->Get(cm2);

  Message check_message2;
  EXPECT_EQ(reply_queue.Size(), 1);
  reply_queue.WaitAndPop(&check_message2);
  auto rep_keys2 = third_party::SArray<int>(check_message2.data[0]);
  auto rep_vals2 = third_party::SArray<int>(check_message2.data[1]);
  EXPECT_EQ(rep_keys2.size(), 1);
  EXPECT_EQ(rep_vals2.size(), 1);
  EXPECT_EQ(rep_vals2[0], 100);
}

TEST_F(TestBSPModel, CheckClock) {
  ThreadsafeQueue<Message> reply_queue;
  std::vector<uint32_t> tids{2, 3};
  int model_id = 0;
  std::unique_ptr<AbstractStorage> storage(new Storage<int>());
  std::unique_ptr<AbstractModel> model(new BSPModel(model_id, std::move(storage), &reply_queue));
  model->ResetWorker(tids);

}

}  // namespace
}  // namespace flexps
