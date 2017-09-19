#include "glog/logging.h"
#include "gtest/gtest.h"

#include "base/threadsafe_queue.hpp"
#include "server/sparse_ssp_model.hpp"

namespace flexps {
namespace {

class TestSparseSSPModel : public testing::Test {
 public:
  TestSparseSSPModel() {}
  ~TestSparseSSPModel() {}

 protected:
  void SetUp() {}
  void TearDown() {}
};

TEST_F(TestSparseSSPModel, CheckConstructor) {
  const int model_id = 0;
  const int staleness = 2;
  const int speculation = 2;
  ThreadsafeQueue<Message> reply_queue;

  std::unique_ptr<AbstractStorage> storage(new Storage<int>());
  std::unique_ptr<AbstractModel> model(
      new SparseSSPModel(model_id, std::move(storage), &reply_queue, staleness, speculation));
}

TEST_F(TestSparseSSPModel, SArrayCasting) {
  third_party::SArray<uint32_t> test({0});
  auto hehe = third_party::SArray<char>(test);
  auto haha = third_party::SArray<uint32_t>(hehe);
  EXPECT_EQ(haha[0], 0);
}

TEST_F(TestSparseSSPModel, GetAndAdd) {
  const int model_id = 0;
  const int staleness = 2;
  const int speculation = 2;
  ThreadsafeQueue<Message> reply_queue;
  std::unique_ptr<AbstractStorage> storage(new Storage<int>());
  std::unique_ptr<AbstractModel> model(
      new SparseSSPModel(model_id, std::move(storage), &reply_queue, staleness, speculation));
  Message reset_msg;
  third_party::SArray<uint32_t> tids({2, 3});
  reset_msg.AddData(tids);
  model->ResetWorker(reset_msg);
  Message reset_reply_msg;
  reply_queue.WaitAndPop(&reset_reply_msg);
  EXPECT_EQ(reset_reply_msg.meta.flag, Flag::kResetWorkerInModel);

  // for Check use
  Message check_msg;
  auto rep_keys = third_party::SArray<int>();
  auto rep_vals = third_party::SArray<int>();

  // Message3
  Message m3;
  m3.meta.flag = Flag::kAdd;
  m3.meta.model_id = 0;
  m3.meta.sender = 2;
  m3.meta.recver = 0;
  m3.meta.version = 0;
  third_party::SArray<int> m3_keys({0});
  third_party::SArray<int> m3_vals({0});
  m3.AddData(m3_keys);
  m3.AddData(m3_vals);
  model->Add(m3);

  // Message4
  Message m4;
  m4.meta.flag = Flag::kAdd;
  m4.meta.model_id = 0;
  m4.meta.sender = 3;
  m4.meta.recver = 0;
  m4.meta.version = 0;
  third_party::SArray<int> m4_keys({1});
  third_party::SArray<int> m4_vals({1});
  m4.AddData(m4_keys);
  m4.AddData(m4_vals);
  model->Add(m4);

  // Message5
  Message m5;
  m5.meta.flag = Flag::kGet;
  m5.meta.model_id = 0;
  m5.meta.sender = 2;
  m5.meta.recver = 0;
  m5.meta.version = 0;
  third_party::SArray<int> m5_keys({0});
  m5.AddData(m5_keys);
  model->Get(m5);

  // Check
  reply_queue.WaitAndPop(&check_msg);
  EXPECT_EQ(check_msg.data.size(), 2);
  rep_keys = third_party::SArray<int>(check_msg.data[0]);
  rep_vals = third_party::SArray<int>(check_msg.data[1]);
  EXPECT_EQ(rep_keys.size(), 1);
  EXPECT_EQ(rep_vals.size(), 1);
  EXPECT_EQ(rep_keys[0], 0);
  EXPECT_EQ(rep_vals[0], 0);
  EXPECT_EQ(check_msg.meta.sender, 0);
  EXPECT_EQ(check_msg.meta.recver, 2);
  EXPECT_EQ(check_msg.meta.version, 0);

  // Message6
  Message m6;
  m6.meta.flag = Flag::kGet;
  m6.meta.model_id = 0;
  m6.meta.sender = 3;
  m6.meta.recver = 0;
  m6.meta.version = 0;
  third_party::SArray<int> m6_keys({1});
  m6.AddData(m6_keys);
  model.get()->Get(m6);

  reply_queue.WaitAndPop(&check_msg);
  EXPECT_EQ(check_msg.data.size(), 2);
  rep_keys = third_party::SArray<int>(check_msg.data[0]);
  rep_vals = third_party::SArray<int>(check_msg.data[1]);
  EXPECT_EQ(rep_keys.size(), 1);
  EXPECT_EQ(rep_vals.size(), 1);
  EXPECT_EQ(rep_keys[0], 1);
  EXPECT_EQ(rep_vals[0], 1);
  EXPECT_EQ(check_msg.meta.sender, 0);
  EXPECT_EQ(check_msg.meta.recver, 3);
  EXPECT_EQ(check_msg.meta.version, 0);

  // Message 7
  Message m7;
  m7.meta.flag = Flag::kAdd;
  m7.meta.model_id = 0;
  m7.meta.sender = 3;
  m7.meta.recver = 0;
  m7.meta.version = 0;
  third_party::SArray<int> m7_keys({1});
  third_party::SArray<int> m7_vals({1});
  m7.AddData(m7_keys);
  m7.AddData(m7_vals);
  model->Add(m7);

  // Message 8
  Message m8;
  m8.meta.flag = Flag::kGet;
  m8.meta.model_id = 0;
  m8.meta.sender = 3;
  m8.meta.recver = 0;
  m8.meta.version = 1;
  third_party::SArray<int> m8_keys({1});
  m8.AddData(m8_keys);
  model.get()->Get(m8);

  // Message 10
  Message m10;
  m10.meta.flag = Flag::kGet;
  m10.meta.model_id = 0;
  m10.meta.sender = 3;
  m10.meta.recver = 0;
  m10.meta.version = 2;
  third_party::SArray<int> m10_keys({1});
  m10.AddData(m10_keys);
  model.get()->Get(m10);

  // TODO(Ruoyu Wu): How to check no reply

  // Message 9
  Message m11;
  m11.meta.flag = Flag::kClock;
  m11.meta.model_id = 0;
  m11.meta.sender = 3;
  m11.meta.recver = 0;
  model.get()->Clock(m11);

  reply_queue.WaitAndPop(&check_msg);
  EXPECT_EQ(check_msg.data.size(), 2);
  rep_keys = third_party::SArray<int>(check_msg.data[0]);
  rep_vals = third_party::SArray<int>(check_msg.data[1]);
  EXPECT_EQ(rep_keys.size(), 1);
  EXPECT_EQ(rep_vals.size(), 1);
  EXPECT_EQ(rep_keys[0], 1);
  EXPECT_EQ(rep_vals[0], 2);
  EXPECT_EQ(check_msg.meta.sender, 0);
  EXPECT_EQ(check_msg.meta.recver, 3);
  EXPECT_EQ(check_msg.meta.version, 1);

  EXPECT_EQ(reply_queue.Size(), 0);
}

TEST_F(TestSparseSSPModel, SpeculationNoConflict) {
  const int model_id = 0;
  const int staleness = 2;
  const int speculation = 2;
  ThreadsafeQueue<Message> reply_queue;
  std::unique_ptr<AbstractStorage> storage(new Storage<int>());
  std::unique_ptr<AbstractModel> model(
      new SparseSSPModel(model_id, std::move(storage), &reply_queue, staleness, speculation));
  Message reset_msg;
  third_party::SArray<uint32_t> tids({2, 3});
  reset_msg.AddData(tids);
  model->ResetWorker(reset_msg);
  Message reset_reply_msg;
  reply_queue.WaitAndPop(&reset_reply_msg);
  EXPECT_EQ(reset_reply_msg.meta.flag, Flag::kResetWorkerInModel);

  // for Check use
  Message check_msg;
  auto rep_keys = third_party::SArray<int>();
  auto rep_vals = third_party::SArray<int>();

  // Message3
  Message m3;
  m3.meta.flag = Flag::kAdd;
  m3.meta.model_id = 0;
  m3.meta.sender = 2;
  m3.meta.recver = 0;
  m3.meta.version = 0;
  third_party::SArray<int> m3_keys({0});
  third_party::SArray<int> m3_vals({0});
  m3.AddData(m3_keys);
  m3.AddData(m3_vals);
  model->Add(m3);

  // Message4
  Message m4;
  m4.meta.flag = Flag::kAdd;
  m4.meta.model_id = 0;
  m4.meta.sender = 3;
  m4.meta.recver = 0;
  m4.meta.version = 0;
  third_party::SArray<int> m4_keys({1});
  third_party::SArray<int> m4_vals({1});
  m4.AddData(m4_keys);
  m4.AddData(m4_vals);
  model->Add(m4);

  // Message5
  Message m5;
  m5.meta.flag = Flag::kGet;
  m5.meta.model_id = 0;
  m5.meta.sender = 2;
  m5.meta.recver = 0;
  m5.meta.version = 0;
  third_party::SArray<int> m5_keys({0});
  m5.AddData(m5_keys);
  model->Get(m5);
  reply_queue.WaitAndPop(&check_msg);

  // Message6
  Message m6;
  m6.meta.flag = Flag::kGet;
  m6.meta.model_id = 0;
  m6.meta.sender = 2;
  m6.meta.recver = 0;
  m6.meta.version = 1;
  third_party::SArray<int> m6_keys({0});
  m6.AddData(m6_keys);
  model->Get(m6);

  // Message7
  Message m7;
  m7.meta.flag = Flag::kGet;
  m7.meta.model_id = 0;
  m7.meta.sender = 3;
  m7.meta.recver = 0;
  m7.meta.version = 0;
  third_party::SArray<int> m7_keys({1});
  m7.AddData(m7_keys);
  model->Get(m7);
  reply_queue.WaitAndPop(&check_msg);

  // Message8
  Message m8;
  m8.meta.flag = Flag::kGet;
  m8.meta.model_id = 0;
  m8.meta.sender = 3;
  m8.meta.recver = 0;
  m8.meta.version = 1;
  third_party::SArray<int> m8_keys({1});
  m8.AddData(m8_keys);
  model->Get(m8);

  // Message 9
  Message m9;
  m9.meta.flag = Flag::kClock;
  m9.meta.model_id = 0;
  m9.meta.sender = 3;
  m9.meta.recver = 0;
  model.get()->Clock(m9);
  
  reply_queue.WaitAndPop(&check_msg);

  // Message10
  Message m10;
  m10.meta.flag = Flag::kGet;
  m10.meta.model_id = 0;
  m10.meta.sender = 3;
  m10.meta.recver = 0;
  m10.meta.version = 2;
  third_party::SArray<int> m10_keys({1});
  m10.AddData(m10_keys);
  model->Get(m10);

  // Message 11
  Message m11;
  m11.meta.flag = Flag::kClock;
  m11.meta.model_id = 0;
  m11.meta.sender = 3;
  m11.meta.recver = 0;
  model.get()->Clock(m11);

  reply_queue.WaitAndPop(&check_msg);
  EXPECT_EQ(check_msg.meta.version, 2);

  // Message12
  Message m12;
  m12.meta.flag = Flag::kGet;
  m12.meta.model_id = 0;
  m12.meta.sender = 3;
  m12.meta.recver = 0;
  m12.meta.version = 3;
  third_party::SArray<int> m12_keys({1});
  m12.AddData(m12_keys);
  model->Get(m12);

  // Message12
  Message m0;
  m0.meta.flag = Flag::kGet;
  m0.meta.model_id = 0;
  m0.meta.sender = 3;
  m0.meta.recver = 0;
  m0.meta.version = 4;
  third_party::SArray<int> m0_keys({1});
  m0.AddData(m0_keys);
  model->Get(m0);

  // Message 13
  Message m13;
  m13.meta.flag = Flag::kClock;
  m13.meta.model_id = 0;
  m13.meta.sender = 3;
  m13.meta.recver = 0;
  model.get()->Clock(m13);

  EXPECT_EQ(model->GetProgress(3), 3);

  // check
  reply_queue.WaitAndPop(&check_msg);
  EXPECT_EQ(check_msg.data.size(), 2);
  rep_keys = third_party::SArray<int>(check_msg.data[0]);
  rep_vals = third_party::SArray<int>(check_msg.data[1]);
  EXPECT_EQ(rep_keys.size(), 1);
  EXPECT_EQ(rep_vals.size(), 1);
  EXPECT_EQ(rep_keys[0], 1);
  EXPECT_EQ(rep_vals[0], 1);
  EXPECT_EQ(check_msg.meta.sender, 0);
  EXPECT_EQ(check_msg.meta.recver, 3);
  EXPECT_EQ(check_msg.meta.version, 3);

  // Message 14
  Message m14;
  m14.meta.flag = Flag::kClock;
  m14.meta.model_id = 0;
  m14.meta.sender = 3;
  m14.meta.recver = 0;
  model.get()->Clock(m14);

  EXPECT_EQ(model->GetProgress(3), 4);

  // check
  reply_queue.WaitAndPop(&check_msg);
  EXPECT_EQ(check_msg.data.size(), 2);
  rep_keys = third_party::SArray<int>(check_msg.data[0]);
  rep_vals = third_party::SArray<int>(check_msg.data[1]);
  EXPECT_EQ(rep_keys.size(), 1);
  EXPECT_EQ(rep_vals.size(), 1);
  EXPECT_EQ(rep_keys[0], 1);
  EXPECT_EQ(rep_vals[0], 1);
  EXPECT_EQ(check_msg.meta.sender, 0);
  EXPECT_EQ(check_msg.meta.recver, 3);
  EXPECT_EQ(check_msg.meta.version, 4);
}

TEST_F(TestSparseSSPModel, SpeculationSeveralConflict) {
  
}

}  // namespace
}  // namespace flexps
