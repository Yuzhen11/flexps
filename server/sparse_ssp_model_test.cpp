#include "glog/logging.h"
#include "gtest/gtest.h"

#include "base/threadsafe_queue.hpp"
#include "server/sparse_ssp_model.hpp"
#include "server/map_storage.hpp"

#include "server/abstract_sparse_ssp_recorder.hpp"
#include "server/vector_sparse_ssp_recorder.hpp"
#include "server/unordered_map_sparse_ssp_recorder.hpp"

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

TEST_F(TestSparseSSPModel, SArrayCasting) {
  third_party::SArray<uint32_t> test({0});
  auto hehe = third_party::SArray<char>(test);
  auto haha = third_party::SArray<uint32_t>(hehe);
  EXPECT_EQ(haha[0], 0);
}

TEST_F(TestSparseSSPModel, CreateMessage) {
  Message m1 = CreateMessage(Flag::kAdd, 0, 3, 0, 0, {1}, {2});
  EXPECT_EQ(m1.data.size(), 2);
  auto rep_keys = third_party::SArray<int>(m1.data[0]);
  auto rep_vals = third_party::SArray<int>(m1.data[1]);
  EXPECT_EQ(m1.meta.flag, Flag::kAdd);
  EXPECT_EQ(rep_keys.size(), 1);
  EXPECT_EQ(rep_vals.size(), 1);
  EXPECT_EQ(rep_keys[0], 1);
  EXPECT_EQ(rep_vals[0], 2);

  Message m2 = CreateMessage(Flag::kGet, 0, 3, 0, 0, {3});
  EXPECT_EQ(m2.data.size(), 1);
  rep_keys = third_party::SArray<int>(m2.data[0]);
  EXPECT_EQ(m2.meta.flag, Flag::kGet);
  EXPECT_EQ(rep_keys.size(), 1);
  EXPECT_EQ(rep_keys[0], 3);

  Message m3 = CreateMessage(Flag::kClock, 0, 3, 0, 0);
  EXPECT_EQ(m3.data.size(), 0);
}

TEST_F(TestSparseSSPModel, CheckConstructor) {
  const int model_id = 0;
  const int staleness = 2;
  const int speculation = 2;
  ThreadsafeQueue<Message> reply_queue;

  std::unique_ptr<AbstractStorage> storage(new MapStorage<int>());
  std::unique_ptr<AbstractSparseSSPRecorder> recorder(
      new UnorderedMapSparseSSPRecorder(staleness, speculation));
  std::unique_ptr<AbstractModel> model(
      new SparseSSPModel(model_id, std::move(storage), std::move(recorder), &reply_queue, staleness, speculation));
}

TEST_F(TestSparseSSPModel, GetAndAdd) {
  const int model_id = 0;
  const int staleness = 2;
  const int speculation = 2;
  ThreadsafeQueue<Message> reply_queue;
  std::unique_ptr<AbstractStorage> storage(new MapStorage<int>());
  std::unique_ptr<AbstractSparseSSPRecorder> recorder(
      new UnorderedMapSparseSSPRecorder(staleness, speculation));
  std::unique_ptr<AbstractModel> model(
      new SparseSSPModel(model_id, std::move(storage), std::move(recorder), &reply_queue, staleness, speculation));

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

  // worker2 add 0 to param 0 with version 0
  Message m0 = CreateMessage(Flag::kAdd, 0, 2, 0, 0, {0}, {0});
  model->Add(m0);

  // worker3 add 1 to param 1 with version 0
  Message m1 = CreateMessage(Flag::kAdd, 0, 3, 0, 0, {1}, {1});
  model->Add(m1);

  // worker 2 get 0 from 0 with version 0
  Message m2 = CreateMessage(Flag::kGet, 0, 2, 0, 0, {0});
  model->Get(m2);

  // // Check
  // reply_queue.WaitAndPop(&check_msg);
  // EXPECT_EQ(check_msg.data.size(), 2);
  // rep_keys = third_party::SArray<int>(check_msg.data[0]);
  // rep_vals = third_party::SArray<int>(check_msg.data[1]);
  // EXPECT_EQ(rep_keys.size(), 1);
  // EXPECT_EQ(rep_vals.size(), 1);
  // EXPECT_EQ(rep_keys[0], 0);
  // EXPECT_EQ(rep_vals[0], 0);
  // EXPECT_EQ(check_msg.meta.sender, 0);
  // EXPECT_EQ(check_msg.meta.recver, 2);
  // EXPECT_EQ(check_msg.meta.version, 0);

  // // Message 3
  // Message m3 = CreateMessage(Flag::kGet, 0, 3, 0, 0, {1});
  // model->Get(m3);

  // reply_queue.WaitAndPop(&check_msg);
  // EXPECT_EQ(check_msg.data.size(), 2);
  // rep_keys = third_party::SArray<int>(check_msg.data[0]);
  // rep_vals = third_party::SArray<int>(check_msg.data[1]);
  // EXPECT_EQ(rep_keys.size(), 1);
  // EXPECT_EQ(rep_vals.size(), 1);
  // EXPECT_EQ(rep_keys[0], 1);
  // EXPECT_EQ(rep_vals[0], 1);
  // EXPECT_EQ(check_msg.meta.sender, 0);
  // EXPECT_EQ(check_msg.meta.recver, 3);
  // EXPECT_EQ(check_msg.meta.version, 0);

  // // Message 4
  // Message m4 = CreateMessage(Flag::kAdd, 0, 3, 0, 0, {1}, {1});
  // model->Add(m4);

  // // Message 5
  // Message m5 = CreateMessage(Flag::kGet, 0, 3, 0, 1, {1});
  // model->Get(m5);

  // // Message 6
  // Message m6 = CreateMessage(Flag::kGet, 0, 3, 0, 2, {1});
  // model->Get(m6);

  // // TODO(Ruoyu Wu): How to check no reply

  // // Message 7
  // Message m7 = CreateMessage(Flag::kClock, 0, 3, 0, 0);
  // model->Clock(m7);

  // reply_queue.WaitAndPop(&check_msg);
  // EXPECT_EQ(check_msg.data.size(), 2);
  // rep_keys = third_party::SArray<int>(check_msg.data[0]);
  // rep_vals = third_party::SArray<int>(check_msg.data[1]);
  // EXPECT_EQ(rep_keys.size(), 1);
  // EXPECT_EQ(rep_vals.size(), 1);
  // EXPECT_EQ(rep_keys[0], 1);
  // EXPECT_EQ(rep_vals[0], 2);
  // EXPECT_EQ(check_msg.meta.sender, 0);
  // EXPECT_EQ(check_msg.meta.recver, 3);
  // EXPECT_EQ(check_msg.meta.version, 1);

  // EXPECT_EQ(reply_queue.Size(), 0);
}

TEST_F(TestSparseSSPModel, SpeculationNoConflict) {
  const int model_id = 0;
  const int staleness = 2;
  const int speculation = 2;
  ThreadsafeQueue<Message> reply_queue;
  std::unique_ptr<AbstractStorage> storage(new MapStorage<int>());
  std::unique_ptr<AbstractSparseSSPRecorder> recorder(
      new UnorderedMapSparseSSPRecorder(staleness, speculation));
  std::unique_ptr<AbstractModel> model(
      new SparseSSPModel(model_id, std::move(storage), std::move(recorder), &reply_queue, staleness, speculation));

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

  // Message1
  Message m1 = CreateMessage(Flag::kAdd, 0, 2, 0, 0, {0}, {0});;
  model->Add(m1);

  // Message2
  Message m2 = CreateMessage(Flag::kAdd, 0, 3, 0, 0, {1}, {1});;
  model->Add(m2);

  // Message3
  Message m3 = CreateMessage(Flag::kGet, 0, 2, 0, 0, {0});
  model->Get(m3);
  reply_queue.WaitAndPop(&check_msg);

  // Message4
  Message m4 = CreateMessage(Flag::kGet, 0, 3, 0, 0, {1});
  model->Get(m4);

  // Message5
  Message m5 = CreateMessage(Flag::kGet, 0, 2, 0, 1, {0});;
  model->Get(m5);
  reply_queue.WaitAndPop(&check_msg);

  // Message6
  Message m6 = CreateMessage(Flag::kGet, 0, 3, 0, 1, {1});;
  model->Get(m6);

  // Message 7
  Message m7 = CreateMessage(Flag::kClock, 0, 3, 0, 0);;
  model.get()->Clock(m7);
  
  reply_queue.WaitAndPop(&check_msg);

  // Message 8
  Message m8 = CreateMessage(Flag::kGet, 0, 3, 0, 2, {1});;
  model->Get(m8);

  // Message 9
  Message m9 = CreateMessage(Flag::kClock, 0, 3, 0, 1);
  model.get()->Clock(m9);

  reply_queue.WaitAndPop(&check_msg);
  EXPECT_EQ(check_msg.meta.version, 2);

  // Message 10
  Message m10 = CreateMessage(Flag::kGet, 0, 3, 0, 3, {1});;
  model->Get(m10);

  // Message11
  Message m11 = CreateMessage(Flag::kGet, 0, 3, 0, 4, {1});;
  model->Get(m11);

  // Message 12
  Message m12 = CreateMessage(Flag::kClock, 0, 3, 0, 2);;
  model->Clock(m12);

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

  // Message 13
  Message m13 = CreateMessage(Flag::kClock, 0, 3, 0, 3);
  model.get()->Clock(m13);

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

// staleness = 0
// speculation = 1 (Send next Get before Clock)
// The two threads are accessing the same key.
// Get_i_j represents the jth Get from thread i
//
// Thread 2              Thread 3
// Get_2_0 (reply)      Get_3_0 (reply)
//
// Get_2_1
// Clock (no reply due to Get_2_1 conflict with Get_3_0)
//
//                      Get_3_1
//                      Clock (2 replies since they are now in next iter)
//
TEST_F(TestSparseSSPModel, staleness0speculation1Conflict) {
  const int model_id = 0;
  const int staleness = 0;
  const int speculation = 1;
  ThreadsafeQueue<Message> reply_queue;
  std::unique_ptr<AbstractStorage> storage(new MapStorage<int>());
  std::unique_ptr<AbstractSparseSSPRecorder> recorder(
      new UnorderedMapSparseSSPRecorder(staleness, speculation));
  std::unique_ptr<AbstractModel> model(
      new SparseSSPModel(model_id, std::move(storage), std::move(recorder), &reply_queue, staleness, speculation));

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

  Message msg;
  // Get_2_0
  msg = CreateMessage(Flag::kGet, 0, 2, 0, 0, {0});
  model->Get(msg);
  // Get_3_0
  msg = CreateMessage(Flag::kGet, 0, 3, 0, 0, {0});
  model->Get(msg);

  reply_queue.WaitAndPop(&check_msg);
  EXPECT_EQ(check_msg.meta.flag, Flag::kGet);
  EXPECT_EQ(check_msg.meta.sender, 0);
  EXPECT_EQ(check_msg.meta.recver, 2);
  EXPECT_EQ(check_msg.meta.version, 0);
  EXPECT_EQ(check_msg.data.size(), 2);
  rep_keys = third_party::SArray<int>(check_msg.data[0]);
  rep_vals = third_party::SArray<int>(check_msg.data[1]);
  EXPECT_EQ(rep_keys.size(), 1);
  EXPECT_EQ(rep_vals.size(), 1);
  EXPECT_EQ(rep_keys[0], 0);
  EXPECT_EQ(rep_vals[0], 0);

  reply_queue.WaitAndPop(&check_msg);
  EXPECT_EQ(check_msg.meta.flag, Flag::kGet);
  EXPECT_EQ(check_msg.meta.version, 0);
  EXPECT_EQ(check_msg.meta.sender, 0);
  EXPECT_EQ(check_msg.meta.recver, 3);
  EXPECT_EQ(check_msg.data.size(), 2);
  rep_keys = third_party::SArray<int>(check_msg.data[0]);
  rep_vals = third_party::SArray<int>(check_msg.data[1]);
  EXPECT_EQ(rep_keys.size(), 1);
  EXPECT_EQ(rep_vals.size(), 1);
  EXPECT_EQ(rep_keys[0], 0);
  EXPECT_EQ(rep_vals[0], 0);

  // Get_2_1 and Clock
  msg = CreateMessage(Flag::kGet, 0, 2, 0, 1, {0});
  model->Get(msg);
  msg = CreateMessage(Flag::kClock, 0, 2, 0, 0);
  model->Clock(msg);

  EXPECT_EQ(reply_queue.Size(), 0);  // No reply since it is blocked by staleness and sparse conflict.

  // Get_3_1 and Clock
  msg = CreateMessage(Flag::kGet, 0, 3, 0, 1, {0});
  model->Get(msg);
  msg = CreateMessage(Flag::kClock, 0, 3, 0, 0);
  model->Clock(msg);

  EXPECT_EQ(reply_queue.Size(), 2);  // 2 replies
}

// staleness = 0
// speculation = 1 (Send next Get before Clock)
// The two threads are accessing the same key.
// Get_i_j represents the jth Get from thread i
//
// Thread 2              Thread 3
// Get_2_0 (reply)      Get_3_0 (reply)
//
// Get_2_1
// Clock (reply since Get_2_1 does not conflict with Get_3_0)
//
// Get_2_2
// Clock (no reply since the server does not know Get_3_1 yet)
//                      Get_3_1
//                      Clock (2 replies if Get_2_2 does not conflict with G_3_1)
//
TEST_F(TestSparseSSPModel, staleness0speculation1NoConflictCase1) {
  const int model_id = 0;
  const int staleness = 0;
  const int speculation = 1;
  ThreadsafeQueue<Message> reply_queue;
  std::unique_ptr<AbstractStorage> storage(new MapStorage<int>());
  std::unique_ptr<AbstractSparseSSPRecorder> recorder(
      new UnorderedMapSparseSSPRecorder(staleness, speculation));
  std::unique_ptr<AbstractModel> model(
      new SparseSSPModel(model_id, std::move(storage), std::move(recorder), &reply_queue, staleness, speculation));

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

  Message msg;
  // Get_2_0
  msg = CreateMessage(Flag::kGet, 0, 2, 0, 0, {0});
  model->Get(msg);
  // Get_3_0
  msg = CreateMessage(Flag::kGet, 0, 3, 0, 0, {1});
  model->Get(msg);

  reply_queue.WaitAndPop(&check_msg);
  EXPECT_EQ(check_msg.meta.flag, Flag::kGet);
  EXPECT_EQ(check_msg.meta.sender, 0);
  EXPECT_EQ(check_msg.meta.recver, 2);
  EXPECT_EQ(check_msg.meta.version, 0);
  EXPECT_EQ(check_msg.data.size(), 2);
  rep_keys = third_party::SArray<int>(check_msg.data[0]);
  rep_vals = third_party::SArray<int>(check_msg.data[1]);
  EXPECT_EQ(rep_keys.size(), 1);
  EXPECT_EQ(rep_vals.size(), 1);
  EXPECT_EQ(rep_keys[0], 0);
  EXPECT_EQ(rep_vals[0], 0);

  reply_queue.WaitAndPop(&check_msg);
  EXPECT_EQ(check_msg.meta.flag, Flag::kGet);
  EXPECT_EQ(check_msg.meta.version, 0);
  EXPECT_EQ(check_msg.meta.sender, 0);
  EXPECT_EQ(check_msg.meta.recver, 3);
  EXPECT_EQ(check_msg.data.size(), 2);
  rep_keys = third_party::SArray<int>(check_msg.data[0]);
  rep_vals = third_party::SArray<int>(check_msg.data[1]);
  EXPECT_EQ(rep_keys.size(), 1);
  EXPECT_EQ(rep_vals.size(), 1);
  EXPECT_EQ(rep_keys[0], 1);
  EXPECT_EQ(rep_vals[0], 0);

  // Get_2_1 and Clock
  msg = CreateMessage(Flag::kGet, 0, 2, 0, 1, {0});  // Get_2_1 is different from Get_3_0
  model->Get(msg);
  msg = CreateMessage(Flag::kClock, 0, 2, 0, 0);
  model->Clock(msg);

  EXPECT_EQ(reply_queue.Size(), 1);
  reply_queue.WaitAndPop(&check_msg);
  EXPECT_EQ(check_msg.meta.flag, Flag::kGet);
  EXPECT_EQ(check_msg.meta.version, 1);
  EXPECT_EQ(check_msg.meta.sender, 0);
  EXPECT_EQ(check_msg.meta.recver, 2);
  EXPECT_EQ(check_msg.data.size(), 2);
  rep_keys = third_party::SArray<int>(check_msg.data[0]);
  rep_vals = third_party::SArray<int>(check_msg.data[1]);
  EXPECT_EQ(rep_keys.size(), 1);
  EXPECT_EQ(rep_vals.size(), 1);
  EXPECT_EQ(rep_keys[0], 0);
  EXPECT_EQ(rep_vals[0], 0);

  // Get_2_2 and Clock
  msg = CreateMessage(Flag::kGet, 0, 2, 0, 2, {0});
  model->Get(msg);
  msg = CreateMessage(Flag::kClock, 0, 2, 0, 1);
  model->Clock(msg);

  EXPECT_EQ(reply_queue.Size(), 0);

  // Get_3_1 and Clock
  msg = CreateMessage(Flag::kGet, 0, 3, 0, 1, {1});  // Get_3_1 is different from Get_2_2
  model->Get(msg);
  msg = CreateMessage(Flag::kClock, 0, 3, 0, 0);
  model->Clock(msg);

  EXPECT_EQ(reply_queue.Size(), 2);  // 2 replies
}

// staleness = 0
// speculation = 1 (Send next Get before Clock)
// The two threads are accessing the same key.
// Get_i_j represents the jth Get from thread i
//
// Thread 2              Thread 3
// Get_2_0 (reply)      Get_3_0 (reply)
//
// Get_2_1
// Clock (reply since Get_2_1 does not conflict with Get_3_0)
//
// Get_2_2
// Clock (no reply since the server does not know Get_3_1 yet)
//                      Get_3_1
//                      Clock (1 replies (Get_3_1) if Get_2_2 conflicts with G_3_1)
//
TEST_F(TestSparseSSPModel, staleness0speculation1NoConflictCase2) {
  const int model_id = 0;
  const int staleness = 0;
  const int speculation = 1;
  ThreadsafeQueue<Message> reply_queue;
  std::unique_ptr<AbstractStorage> storage(new MapStorage<int>());
  std::unique_ptr<AbstractSparseSSPRecorder> recorder(
      new UnorderedMapSparseSSPRecorder(staleness, speculation));
  std::unique_ptr<AbstractModel> model(
      new SparseSSPModel(model_id, std::move(storage), std::move(recorder), &reply_queue, staleness, speculation));

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

  Message msg;
  // Get_2_0
  msg = CreateMessage(Flag::kGet, 0, 2, 0, 0, {0});
  model->Get(msg);
  // Get_3_0
  msg = CreateMessage(Flag::kGet, 0, 3, 0, 0, {1});
  model->Get(msg);

  reply_queue.WaitAndPop(&check_msg);
  EXPECT_EQ(check_msg.meta.flag, Flag::kGet);
  EXPECT_EQ(check_msg.meta.sender, 0);
  EXPECT_EQ(check_msg.meta.recver, 2);
  EXPECT_EQ(check_msg.meta.version, 0);
  EXPECT_EQ(check_msg.data.size(), 2);
  rep_keys = third_party::SArray<int>(check_msg.data[0]);
  rep_vals = third_party::SArray<int>(check_msg.data[1]);
  EXPECT_EQ(rep_keys.size(), 1);
  EXPECT_EQ(rep_vals.size(), 1);
  EXPECT_EQ(rep_keys[0], 0);
  EXPECT_EQ(rep_vals[0], 0);

  reply_queue.WaitAndPop(&check_msg);
  EXPECT_EQ(check_msg.meta.flag, Flag::kGet);
  EXPECT_EQ(check_msg.meta.version, 0);
  EXPECT_EQ(check_msg.meta.sender, 0);
  EXPECT_EQ(check_msg.meta.recver, 3);
  EXPECT_EQ(check_msg.data.size(), 2);
  rep_keys = third_party::SArray<int>(check_msg.data[0]);
  rep_vals = third_party::SArray<int>(check_msg.data[1]);
  EXPECT_EQ(rep_keys.size(), 1);
  EXPECT_EQ(rep_vals.size(), 1);
  // Ruoyu Wu
  EXPECT_EQ(rep_keys[0], 1);
  EXPECT_EQ(rep_vals[0], 0);

  // Get_2_1 and Clock
  msg = CreateMessage(Flag::kGet, 0, 2, 0, 1, {0});  // Get_2_1 is different from Get_3_0
  model->Get(msg);
  msg = CreateMessage(Flag::kClock, 0, 2, 0, 0);
  model->Clock(msg);

  EXPECT_EQ(reply_queue.Size(), 1);
  reply_queue.WaitAndPop(&check_msg);
  EXPECT_EQ(check_msg.meta.flag, Flag::kGet);
  EXPECT_EQ(check_msg.meta.version, 1);
  EXPECT_EQ(check_msg.meta.sender, 0);
  EXPECT_EQ(check_msg.meta.recver, 2);
  EXPECT_EQ(check_msg.data.size(), 2);
  rep_keys = third_party::SArray<int>(check_msg.data[0]);
  rep_vals = third_party::SArray<int>(check_msg.data[1]);
  EXPECT_EQ(rep_keys.size(), 1);
  EXPECT_EQ(rep_vals.size(), 1);
  EXPECT_EQ(rep_keys[0], 0);
  EXPECT_EQ(rep_vals[0], 0);

  // Get_2_2 and Clock
  msg = CreateMessage(Flag::kGet, 0, 2, 0, 2, {0});
  model->Get(msg);
  msg = CreateMessage(Flag::kClock, 0, 2, 0, 1);
  model->Clock(msg);

  EXPECT_EQ(reply_queue.Size(), 0);

  // Get_3_1 and Clock
  msg = CreateMessage(Flag::kGet, 0, 3, 0, 1, {0});  // Get_3_1 is the same as Get_2_2
  model->Get(msg);
  msg = CreateMessage(Flag::kClock, 0, 3, 0, 0);
  model->Clock(msg);

  EXPECT_EQ(reply_queue.Size(), 1);
  reply_queue.WaitAndPop(&check_msg);
  EXPECT_EQ(check_msg.meta.flag, Flag::kGet);
  EXPECT_EQ(check_msg.meta.version, 1);
  EXPECT_EQ(check_msg.meta.sender, 0);
  EXPECT_EQ(check_msg.meta.recver, 3);
  EXPECT_EQ(check_msg.data.size(), 2);
  rep_keys = third_party::SArray<int>(check_msg.data[0]);
  rep_vals = third_party::SArray<int>(check_msg.data[1]);
  EXPECT_EQ(rep_keys.size(), 1);
  EXPECT_EQ(rep_vals.size(), 1);
  EXPECT_EQ(rep_keys[0], 0);
  EXPECT_EQ(rep_vals[0], 0);
}

// staleness = 0
// speculation = 2 (Send the Get in the next 2 iterations before Clock)
// The two threads are accessing the same key.
// Get_i_j represents the jth Get from thread i
//
// Thread 2              Thread 3
// Get_2_0 (reply)      Get_3_0 (reply)
// Get_2_1              Get_3_1
//
// Get_2_2
// Clock (no reply due to Get_2_1 conflict with Get_3_0)
//
//                      Get_3_2
//                      Clock (2 replies since they are now in next iter)
//
TEST_F(TestSparseSSPModel, staleness0speculation2Conflict) {
  const int model_id = 0;
  const int staleness = 0;
  const int speculation = 2;
  ThreadsafeQueue<Message> reply_queue;
  std::unique_ptr<AbstractStorage> storage(new MapStorage<int>());
  std::unique_ptr<AbstractSparseSSPRecorder> recorder(
      new UnorderedMapSparseSSPRecorder(staleness, speculation));
  std::unique_ptr<AbstractModel> model(
      new SparseSSPModel(model_id, std::move(storage), std::move(recorder), &reply_queue, staleness, speculation));

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

  Message msg;
  // Get_2_0
  msg = CreateMessage(Flag::kGet, 0, 2, 0, 0, {0});
  model->Get(msg);
  // Get_3_0
  msg = CreateMessage(Flag::kGet, 0, 3, 0, 0, {0});
  model->Get(msg);

  reply_queue.WaitAndPop(&check_msg);
  EXPECT_EQ(check_msg.meta.flag, Flag::kGet);
  EXPECT_EQ(check_msg.meta.sender, 0);
  EXPECT_EQ(check_msg.meta.recver, 2);
  EXPECT_EQ(check_msg.meta.version, 0);
  EXPECT_EQ(check_msg.data.size(), 2);
  rep_keys = third_party::SArray<int>(check_msg.data[0]);
  rep_vals = third_party::SArray<int>(check_msg.data[1]);
  EXPECT_EQ(rep_keys.size(), 1);
  EXPECT_EQ(rep_vals.size(), 1);
  EXPECT_EQ(rep_keys[0], 0);
  EXPECT_EQ(rep_vals[0], 0);

  reply_queue.WaitAndPop(&check_msg);
  EXPECT_EQ(check_msg.meta.flag, Flag::kGet);
  EXPECT_EQ(check_msg.meta.version, 0);
  EXPECT_EQ(check_msg.meta.sender, 0);
  EXPECT_EQ(check_msg.meta.recver, 3);
  EXPECT_EQ(check_msg.data.size(), 2);
  rep_keys = third_party::SArray<int>(check_msg.data[0]);
  rep_vals = third_party::SArray<int>(check_msg.data[1]);
  EXPECT_EQ(rep_keys.size(), 1);
  EXPECT_EQ(rep_vals.size(), 1);
  EXPECT_EQ(rep_keys[0], 0);
  EXPECT_EQ(rep_vals[0], 0);

  // Get_2_1
  msg = CreateMessage(Flag::kGet, 0, 2, 0, 1, {0});
  model->Get(msg);
  // Get_3_1
  msg = CreateMessage(Flag::kGet, 0, 3, 0, 1, {0});
  model->Get(msg);

  // Get_2_2 and Clock
  msg = CreateMessage(Flag::kGet, 0, 2, 0, 2, {0});
  model->Get(msg);
  msg = CreateMessage(Flag::kClock, 0, 2, 0, 0);
  model->Clock(msg);

  EXPECT_EQ(reply_queue.Size(), 0);

  // Get_3_2 and Clock
  msg = CreateMessage(Flag::kGet, 0, 3, 0, 2, {0});
  model->Get(msg);
  msg = CreateMessage(Flag::kClock, 0, 3, 0, 0);
  model->Clock(msg);

  EXPECT_EQ(reply_queue.Size(), 2);  // 2 replies
}

// staleness = 0
// speculation = 2 (Send the Get in the next 2 iterations before Clock)
// The two threads are accessing the different key.
// Get_i_j represents the jth Get from thread i
//
// Thread 2              Thread 3
// Get_2_0 [0](reply)      Get_3_0 [1](reply)
// Get_2_1 [0]             Get_3_1 [1]
//
// Get_2_2 [0]
// Clock (reply since Get_2_1 does not conflict with Get_3_0)
//
// Get_2_3 [0]
// Clock (reply since Get_2_2 does not conflict with Get_3_0 and Get_3_1)
//
// Get_2_4 [0]
// Clock (No reply)
//
TEST_F(TestSparseSSPModel, staleness0speculation2Case1) {
  const int model_id = 0;
  const int staleness = 0;
  const int speculation = 2;
  ThreadsafeQueue<Message> reply_queue;
  std::unique_ptr<AbstractStorage> storage(new MapStorage<int>());
  std::unique_ptr<AbstractSparseSSPRecorder> recorder(
      new UnorderedMapSparseSSPRecorder(staleness, speculation));
  std::unique_ptr<AbstractModel> model(
      new SparseSSPModel(model_id, std::move(storage), std::move(recorder), &reply_queue, staleness, speculation));

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

  Message msg;
  // Get_2_0
  msg = CreateMessage(Flag::kGet, 0, 2, 0, 0, {0});
  model->Get(msg);
  // Get_3_0
  msg = CreateMessage(Flag::kGet, 0, 3, 0, 0, {1});
  model->Get(msg);

  reply_queue.WaitAndPop(&check_msg);
  EXPECT_EQ(check_msg.meta.flag, Flag::kGet);
  EXPECT_EQ(check_msg.meta.sender, 0);
  EXPECT_EQ(check_msg.meta.recver, 2);
  EXPECT_EQ(check_msg.meta.version, 0);
  EXPECT_EQ(check_msg.data.size(), 2);
  rep_keys = third_party::SArray<int>(check_msg.data[0]);
  rep_vals = third_party::SArray<int>(check_msg.data[1]);
  EXPECT_EQ(rep_keys.size(), 1);
  EXPECT_EQ(rep_vals.size(), 1);
  EXPECT_EQ(rep_keys[0], 0);
  EXPECT_EQ(rep_vals[0], 0);

  reply_queue.WaitAndPop(&check_msg);
  EXPECT_EQ(check_msg.meta.flag, Flag::kGet);
  EXPECT_EQ(check_msg.meta.version, 0);
  EXPECT_EQ(check_msg.meta.sender, 0);
  EXPECT_EQ(check_msg.meta.recver, 3);
  EXPECT_EQ(check_msg.data.size(), 2);
  rep_keys = third_party::SArray<int>(check_msg.data[0]);
  rep_vals = third_party::SArray<int>(check_msg.data[1]);
  EXPECT_EQ(rep_keys.size(), 1);
  EXPECT_EQ(rep_vals.size(), 1);
  EXPECT_EQ(rep_keys[0], 1);
  EXPECT_EQ(rep_vals[0], 0);

  // Get_2_1
  msg = CreateMessage(Flag::kGet, 0, 2, 0, 1, {0});
  model->Get(msg);
  // Get_3_1
  msg = CreateMessage(Flag::kGet, 0, 3, 0, 1, {1});
  model->Get(msg);

  // Get_2_2 and Clock
  msg = CreateMessage(Flag::kGet, 0, 2, 0, 2, {0});
  model->Get(msg);
  msg = CreateMessage(Flag::kClock, 0, 2, 0, 0);
  model->Clock(msg);

  EXPECT_EQ(reply_queue.Size(), 1);
  reply_queue.WaitAndPop(&check_msg);
  EXPECT_EQ(check_msg.meta.flag, Flag::kGet);
  EXPECT_EQ(check_msg.meta.sender, 0);
  EXPECT_EQ(check_msg.meta.recver, 2);
  EXPECT_EQ(check_msg.meta.version, 1);
  EXPECT_EQ(check_msg.data.size(), 2);
  rep_keys = third_party::SArray<int>(check_msg.data[0]);
  rep_vals = third_party::SArray<int>(check_msg.data[1]);
  EXPECT_EQ(rep_keys.size(), 1);
  EXPECT_EQ(rep_vals.size(), 1);
  EXPECT_EQ(rep_keys[0], 0);
  EXPECT_EQ(rep_vals[0], 0);

  // Get_2_3 and Clock
  msg = CreateMessage(Flag::kGet, 0, 2, 0, 3, {0});
  model->Get(msg);
  msg = CreateMessage(Flag::kClock, 0, 2, 0, 1);
  model->Clock(msg);

  EXPECT_EQ(reply_queue.Size(), 1);
  reply_queue.WaitAndPop(&check_msg);
  EXPECT_EQ(check_msg.meta.flag, Flag::kGet);
  EXPECT_EQ(check_msg.meta.sender, 0);
  EXPECT_EQ(check_msg.meta.recver, 2);
  EXPECT_EQ(check_msg.meta.version, 2);
  EXPECT_EQ(check_msg.data.size(), 2);
  rep_keys = third_party::SArray<int>(check_msg.data[0]);
  rep_vals = third_party::SArray<int>(check_msg.data[1]);
  EXPECT_EQ(rep_keys.size(), 1);
  EXPECT_EQ(rep_vals.size(), 1);
  EXPECT_EQ(rep_keys[0], 0);
  EXPECT_EQ(rep_vals[0], 0);

  // Get_2_4 and Clock
  msg = CreateMessage(Flag::kGet, 0, 2, 0, 4, {1});
  model->Get(msg);
  msg = CreateMessage(Flag::kClock, 0, 2, 0, 2);
  model->Clock(msg);

  EXPECT_EQ(reply_queue.Size(), 0);
}

// staleness = 0
// speculation = 2 (Send the Get in the next 2 iterations before Clock)
// The two threads are accessing the different key.
// Get_i_j represents the jth Get from thread i
//
// Thread 2              Thread 3
// Get_2_0 [0](reply)      Get_3_0 [1](reply)
// Get_2_1 [0]             Get_3_1 [1]
//
// Get_2_2 [0]
// Clock (reply since Get_2_1 does not conflict with Get_3_0)
//
// Get_2_3 [0]
// Clock (reply since Get_2_2 does not conflict with Get_3_0 and Get_3_1)
//
// Get_2_4 [0]
// Clock (No reply)
//                        Get_3_2 [1]
//                        Clock (Reply Get_3_1 and Get_2_3 (No conflict between Get_2_3 with Get_3_1 and Get_3_2))
//
TEST_F(TestSparseSSPModel, staleness0speculation2Case2) {
  const int model_id = 0;
  const int staleness = 0;
  const int speculation = 2;
  ThreadsafeQueue<Message> reply_queue;
  std::unique_ptr<AbstractStorage> storage(new MapStorage<int>());
  std::unique_ptr<AbstractSparseSSPRecorder> recorder(
      new UnorderedMapSparseSSPRecorder(staleness, speculation));
  std::unique_ptr<AbstractModel> model(
      new SparseSSPModel(model_id, std::move(storage), std::move(recorder), &reply_queue, staleness, speculation));

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

  Message msg;
  // Get_2_0
  msg = CreateMessage(Flag::kGet, 0, 2, 0, 0, {0});
  model->Get(msg);
  // Get_3_0
  msg = CreateMessage(Flag::kGet, 0, 3, 0, 0, {1});
  model->Get(msg);

  reply_queue.WaitAndPop(&check_msg);
  EXPECT_EQ(check_msg.meta.flag, Flag::kGet);
  EXPECT_EQ(check_msg.meta.sender, 0);
  EXPECT_EQ(check_msg.meta.recver, 2);
  EXPECT_EQ(check_msg.meta.version, 0);
  EXPECT_EQ(check_msg.data.size(), 2);
  rep_keys = third_party::SArray<int>(check_msg.data[0]);
  rep_vals = third_party::SArray<int>(check_msg.data[1]);
  EXPECT_EQ(rep_keys.size(), 1);
  EXPECT_EQ(rep_vals.size(), 1);
  EXPECT_EQ(rep_keys[0], 0);
  EXPECT_EQ(rep_vals[0], 0);

  reply_queue.WaitAndPop(&check_msg);
  EXPECT_EQ(check_msg.meta.flag, Flag::kGet);
  EXPECT_EQ(check_msg.meta.version, 0);
  EXPECT_EQ(check_msg.meta.sender, 0);
  EXPECT_EQ(check_msg.meta.recver, 3);
  EXPECT_EQ(check_msg.data.size(), 2);
  rep_keys = third_party::SArray<int>(check_msg.data[0]);
  rep_vals = third_party::SArray<int>(check_msg.data[1]);
  EXPECT_EQ(rep_keys.size(), 1);
  EXPECT_EQ(rep_vals.size(), 1);
  EXPECT_EQ(rep_keys[0], 1);
  EXPECT_EQ(rep_vals[0], 0);

  // Get_2_1
  msg = CreateMessage(Flag::kGet, 0, 2, 0, 1, {0});
  model->Get(msg);
  // Get_3_1
  msg = CreateMessage(Flag::kGet, 0, 3, 0, 1, {1});
  model->Get(msg);

  // Get_2_2 and Clock
  msg = CreateMessage(Flag::kGet, 0, 2, 0, 2, {0});
  model->Get(msg);
  msg = CreateMessage(Flag::kClock, 0, 2, 0, 0);
  model->Clock(msg);

  EXPECT_EQ(reply_queue.Size(), 1);
  reply_queue.WaitAndPop(&check_msg);
  EXPECT_EQ(check_msg.meta.flag, Flag::kGet);
  EXPECT_EQ(check_msg.meta.sender, 0);
  EXPECT_EQ(check_msg.meta.recver, 2);
  EXPECT_EQ(check_msg.meta.version, 1);
  EXPECT_EQ(check_msg.data.size(), 2);
  rep_keys = third_party::SArray<int>(check_msg.data[0]);
  rep_vals = third_party::SArray<int>(check_msg.data[1]);
  EXPECT_EQ(rep_keys.size(), 1);
  EXPECT_EQ(rep_vals.size(), 1);
  EXPECT_EQ(rep_keys[0], 0);
  EXPECT_EQ(rep_vals[0], 0);

  // Get_2_3 and Clock
  msg = CreateMessage(Flag::kGet, 0, 2, 0, 3, {0});
  model->Get(msg);
  msg = CreateMessage(Flag::kClock, 0, 2, 0, 1);
  model->Clock(msg);

  EXPECT_EQ(reply_queue.Size(), 1);
  reply_queue.WaitAndPop(&check_msg);
  EXPECT_EQ(check_msg.meta.flag, Flag::kGet);
  EXPECT_EQ(check_msg.meta.sender, 0);
  EXPECT_EQ(check_msg.meta.recver, 2);
  EXPECT_EQ(check_msg.meta.version, 2);
  EXPECT_EQ(check_msg.data.size(), 2);
  rep_keys = third_party::SArray<int>(check_msg.data[0]);
  rep_vals = third_party::SArray<int>(check_msg.data[1]);
  EXPECT_EQ(rep_keys.size(), 1);
  EXPECT_EQ(rep_vals.size(), 1);
  EXPECT_EQ(rep_keys[0], 0);
  EXPECT_EQ(rep_vals[0], 0);

  // Get_2_4 and Clock
  msg = CreateMessage(Flag::kGet, 0, 2, 0, 4, {1});
  model->Get(msg);
  msg = CreateMessage(Flag::kClock, 0, 2, 0, 2);
  model->Clock(msg);

  EXPECT_EQ(reply_queue.Size(), 0);

  // Get_3_2 and Clock
  msg = CreateMessage(Flag::kGet, 0, 3, 0, 2, {1});
  model->Get(msg);
  msg = CreateMessage(Flag::kClock, 0, 3, 0, 0);
  model->Clock(msg);

  EXPECT_EQ(reply_queue.Size(), 2);
}

// staleness = 0
// speculation = 2 (Send the Get in the next 2 iterations before Clock)
// The two threads are accessing the different key.
// Get_i_j represents the jth Get from thread i
//
// Thread 2              Thread 3
// Get_2_0 [0](reply)      Get_3_0 [1](reply)
// Get_2_1 [0]             Get_3_1 [1]
//
// Get_2_2 [0]
// Clock (reply since Get_2_1 does not conflict with Get_3_0)
//
// Get_2_3 [0]
// Clock (reply since Get_2_2 does not conflict with Get_3_0 and Get_3_1)
//
// Get_2_4 [0]
// Clock (No reply)
//                        Get_3_2 [0]
//                        Clock (Reply Get_3_1 only. 
//                        Do not reply Get_2_3 due to conflict between Get_2_3 with Get_3_2))
//
TEST_F(TestSparseSSPModel, staleness0speculation2Case3) {
  const int model_id = 0;
  const int staleness = 0;
  const int speculation = 2;
  ThreadsafeQueue<Message> reply_queue;
  std::unique_ptr<AbstractStorage> storage(new MapStorage<int>());
  std::unique_ptr<AbstractSparseSSPRecorder> recorder(
      new UnorderedMapSparseSSPRecorder(staleness, speculation));
  std::unique_ptr<AbstractModel> model(
      new SparseSSPModel(model_id, std::move(storage), std::move(recorder), &reply_queue, staleness, speculation));

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

  Message msg;
  // Get_2_0
  msg = CreateMessage(Flag::kGet, 0, 2, 0, 0, {0});
  model->Get(msg);
  // Get_3_0
  msg = CreateMessage(Flag::kGet, 0, 3, 0, 0, {1});
  model->Get(msg);

  reply_queue.WaitAndPop(&check_msg);
  EXPECT_EQ(check_msg.meta.flag, Flag::kGet);
  EXPECT_EQ(check_msg.meta.sender, 0);
  EXPECT_EQ(check_msg.meta.recver, 2);
  EXPECT_EQ(check_msg.meta.version, 0);
  EXPECT_EQ(check_msg.data.size(), 2);
  rep_keys = third_party::SArray<int>(check_msg.data[0]);
  rep_vals = third_party::SArray<int>(check_msg.data[1]);
  EXPECT_EQ(rep_keys.size(), 1);
  EXPECT_EQ(rep_vals.size(), 1);
  EXPECT_EQ(rep_keys[0], 0);
  EXPECT_EQ(rep_vals[0], 0);

  reply_queue.WaitAndPop(&check_msg);
  EXPECT_EQ(check_msg.meta.flag, Flag::kGet);
  EXPECT_EQ(check_msg.meta.version, 0);
  EXPECT_EQ(check_msg.meta.sender, 0);
  EXPECT_EQ(check_msg.meta.recver, 3);
  EXPECT_EQ(check_msg.data.size(), 2);
  rep_keys = third_party::SArray<int>(check_msg.data[0]);
  rep_vals = third_party::SArray<int>(check_msg.data[1]);
  EXPECT_EQ(rep_keys.size(), 1);
  EXPECT_EQ(rep_vals.size(), 1);
  EXPECT_EQ(rep_keys[0], 1);
  EXPECT_EQ(rep_vals[0], 0);

  // Get_2_1
  msg = CreateMessage(Flag::kGet, 0, 2, 0, 1, {0});
  model->Get(msg);
  // Get_3_1
  msg = CreateMessage(Flag::kGet, 0, 3, 0, 1, {1});
  model->Get(msg);

  // Get_2_2 and Clock
  msg = CreateMessage(Flag::kGet, 0, 2, 0, 2, {0});
  model->Get(msg);
  msg = CreateMessage(Flag::kClock, 0, 2, 0, 0);
  model->Clock(msg);

  EXPECT_EQ(reply_queue.Size(), 1);
  reply_queue.WaitAndPop(&check_msg);
  EXPECT_EQ(check_msg.meta.flag, Flag::kGet);
  EXPECT_EQ(check_msg.meta.sender, 0);
  EXPECT_EQ(check_msg.meta.recver, 2);
  EXPECT_EQ(check_msg.meta.version, 1);
  EXPECT_EQ(check_msg.data.size(), 2);
  rep_keys = third_party::SArray<int>(check_msg.data[0]);
  rep_vals = third_party::SArray<int>(check_msg.data[1]);
  EXPECT_EQ(rep_keys.size(), 1);
  EXPECT_EQ(rep_vals.size(), 1);
  EXPECT_EQ(rep_keys[0], 0);
  EXPECT_EQ(rep_vals[0], 0);

  // Get_2_3 and Clock
  msg = CreateMessage(Flag::kGet, 0, 2, 0, 3, {0});
  model->Get(msg);
  msg = CreateMessage(Flag::kClock, 0, 2, 0, 1);
  model->Clock(msg);

  EXPECT_EQ(reply_queue.Size(), 1);
  reply_queue.WaitAndPop(&check_msg);
  EXPECT_EQ(check_msg.meta.flag, Flag::kGet);
  EXPECT_EQ(check_msg.meta.sender, 0);
  EXPECT_EQ(check_msg.meta.recver, 2);
  EXPECT_EQ(check_msg.meta.version, 2);
  EXPECT_EQ(check_msg.data.size(), 2);
  rep_keys = third_party::SArray<int>(check_msg.data[0]);
  rep_vals = third_party::SArray<int>(check_msg.data[1]);
  EXPECT_EQ(rep_keys.size(), 1);
  EXPECT_EQ(rep_vals.size(), 1);
  EXPECT_EQ(rep_keys[0], 0);
  EXPECT_EQ(rep_vals[0], 0);

  // Get_2_4 and Clock
  msg = CreateMessage(Flag::kGet, 0, 2, 0, 4, {1});
  model->Get(msg);
  msg = CreateMessage(Flag::kClock, 0, 2, 0, 2);
  model->Clock(msg);

  EXPECT_EQ(reply_queue.Size(), 0);

  // Get_3_2 and Clock
  msg = CreateMessage(Flag::kGet, 0, 3, 0, 2, {0});
  model->Get(msg);
  msg = CreateMessage(Flag::kClock, 0, 3, 0, 0);
  model->Clock(msg);

  EXPECT_EQ(reply_queue.Size(), 1);
  reply_queue.WaitAndPop(&check_msg);
}

// staleness = 0
// speculation = 2 (Send the Get in the next 2 iterations before Clock)
// The two threads are accessing the different key.
// Get_i_j represents the jth Get from thread i
//
// Thread 2              Thread 3
// Get_2_0 [0](reply)      Get_3_0 [1](reply)
// Get_2_1 [0]             Get_3_1 [1]
//
// Get_2_2 [0]
// Clock (reply since Get_2_1 does not conflict with Get_3_0)
//
// Get_2_3 [0]
// Clock (reply since Get_2_2 does not conflict with Get_3_0 and Get_3_1)
//
// Get_2_4 [0]
// Clock (No reply)
//                        Get_3_2 [0]
//                        Clock (Reply Get_3_1 only. 
//                        Do not reply Get_2_3 due to conflict between Get_2_3 with Get_3_2))
//
//                        Get_3_3 [0]
//                        Clock (Reply Get_3_2)
//
//                        Get_3_4 [0]
//                        Clock (Reply Get_3_3 and Get_2_3)
//
TEST_F(TestSparseSSPModel, staleness0speculation2Case4) {
  const int model_id = 0;
  const int staleness = 0;
  const int speculation = 2;
  ThreadsafeQueue<Message> reply_queue;
  std::unique_ptr<AbstractStorage> storage(new MapStorage<int>());
  std::unique_ptr<AbstractSparseSSPRecorder> recorder(
      new UnorderedMapSparseSSPRecorder(staleness, speculation));
  std::unique_ptr<AbstractModel> model(
      new SparseSSPModel(model_id, std::move(storage), std::move(recorder), &reply_queue, staleness, speculation));

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

  Message msg;
  // Get_2_0
  msg = CreateMessage(Flag::kGet, 0, 2, 0, 0, {0});
  model->Get(msg);
  // Get_3_0
  msg = CreateMessage(Flag::kGet, 0, 3, 0, 0, {1});
  model->Get(msg);

  reply_queue.WaitAndPop(&check_msg);
  EXPECT_EQ(check_msg.meta.flag, Flag::kGet);
  EXPECT_EQ(check_msg.meta.sender, 0);
  EXPECT_EQ(check_msg.meta.recver, 2);
  EXPECT_EQ(check_msg.meta.version, 0);
  EXPECT_EQ(check_msg.data.size(), 2);
  rep_keys = third_party::SArray<int>(check_msg.data[0]);
  rep_vals = third_party::SArray<int>(check_msg.data[1]);
  EXPECT_EQ(rep_keys.size(), 1);
  EXPECT_EQ(rep_vals.size(), 1);
  EXPECT_EQ(rep_keys[0], 0);
  EXPECT_EQ(rep_vals[0], 0);

  reply_queue.WaitAndPop(&check_msg);
  EXPECT_EQ(check_msg.meta.flag, Flag::kGet);
  EXPECT_EQ(check_msg.meta.version, 0);
  EXPECT_EQ(check_msg.meta.sender, 0);
  EXPECT_EQ(check_msg.meta.recver, 3);
  EXPECT_EQ(check_msg.data.size(), 2);
  rep_keys = third_party::SArray<int>(check_msg.data[0]);
  rep_vals = third_party::SArray<int>(check_msg.data[1]);
  EXPECT_EQ(rep_keys.size(), 1);
  EXPECT_EQ(rep_vals.size(), 1);
  EXPECT_EQ(rep_keys[0], 1);
  EXPECT_EQ(rep_vals[0], 0);

  // Get_2_1
  msg = CreateMessage(Flag::kGet, 0, 2, 0, 1, {0});
  model->Get(msg);
  // Get_3_1
  msg = CreateMessage(Flag::kGet, 0, 3, 0, 1, {1});
  model->Get(msg);

  // Get_2_2 and Clock
  msg = CreateMessage(Flag::kGet, 0, 2, 0, 2, {0});
  model->Get(msg);
  msg = CreateMessage(Flag::kClock, 0, 2, 0, 0);
  model->Clock(msg);

  EXPECT_EQ(reply_queue.Size(), 1);
  reply_queue.WaitAndPop(&check_msg);
  EXPECT_EQ(check_msg.meta.flag, Flag::kGet);
  EXPECT_EQ(check_msg.meta.sender, 0);
  EXPECT_EQ(check_msg.meta.recver, 2);
  EXPECT_EQ(check_msg.meta.version, 1);
  EXPECT_EQ(check_msg.data.size(), 2);
  rep_keys = third_party::SArray<int>(check_msg.data[0]);
  rep_vals = third_party::SArray<int>(check_msg.data[1]);
  EXPECT_EQ(rep_keys.size(), 1);
  EXPECT_EQ(rep_vals.size(), 1);
  EXPECT_EQ(rep_keys[0], 0);
  EXPECT_EQ(rep_vals[0], 0);

  // Get_2_3 and Clock
  msg = CreateMessage(Flag::kGet, 0, 2, 0, 3, {0});
  model->Get(msg);
  msg = CreateMessage(Flag::kClock, 0, 2, 0, 1);
  model->Clock(msg);

  EXPECT_EQ(reply_queue.Size(), 1);
  reply_queue.WaitAndPop(&check_msg);
  EXPECT_EQ(check_msg.meta.flag, Flag::kGet);
  EXPECT_EQ(check_msg.meta.sender, 0);
  EXPECT_EQ(check_msg.meta.recver, 2);
  EXPECT_EQ(check_msg.meta.version, 2);
  EXPECT_EQ(check_msg.data.size(), 2);
  rep_keys = third_party::SArray<int>(check_msg.data[0]);
  rep_vals = third_party::SArray<int>(check_msg.data[1]);
  EXPECT_EQ(rep_keys.size(), 1);
  EXPECT_EQ(rep_vals.size(), 1);
  EXPECT_EQ(rep_keys[0], 0);
  EXPECT_EQ(rep_vals[0], 0);

  // Get_2_4 and Clock
  msg = CreateMessage(Flag::kGet, 0, 2, 0, 4, {1});
  model->Get(msg);
  msg = CreateMessage(Flag::kClock, 0, 2, 0, 2);
  model->Clock(msg);

  EXPECT_EQ(reply_queue.Size(), 0);

  // Get_3_2 and Clock
  msg = CreateMessage(Flag::kGet, 0, 3, 0, 2, {0});
  model->Get(msg);
  msg = CreateMessage(Flag::kClock, 0, 3, 0, 0);
  model->Clock(msg);

  EXPECT_EQ(reply_queue.Size(), 1);
  reply_queue.WaitAndPop(&check_msg);

  // Get_3_3 and Clock
  msg = CreateMessage(Flag::kGet, 0, 3, 0, 3, {0});
  model->Get(msg);
  msg = CreateMessage(Flag::kClock, 0, 3, 0, 1);
  model->Clock(msg);

  EXPECT_EQ(reply_queue.Size(), 1);
  reply_queue.WaitAndPop(&check_msg);

  // Get_3_4 and Clock
  msg = CreateMessage(Flag::kGet, 0, 3, 0, 4, {0});
  model->Get(msg);
  msg = CreateMessage(Flag::kClock, 0, 3, 0, 2);
  model->Clock(msg);

  EXPECT_EQ(reply_queue.Size(), 2);
  reply_queue.WaitAndPop(&check_msg);
  reply_queue.WaitAndPop(&check_msg);
}

// staleness = 0
// speculation = 2 (Send the Get in the next 2 iterations before Clock)
// The two threads are accessing the different key.
// Get_i_j represents the jth Get from thread i
//
// Thread 2              Thread 3
// Get_2_0 [0](reply)      Get_3_0 [1](reply)
// Get_2_1 [0]             Get_3_1 [2]
//
// Get_2_2 [1]
// Clock (reply since Get_2_1 does not conflict with Get_3_0)
//
// Get_2_3 [1]
// Clock (No reply due to conflict between Get_2_2 and Get_3_0)
//
//                         Get_3_2 [1]
//                         Clock (Reply Get_3_1 and Get_2_2)
//
TEST_F(TestSparseSSPModel, staleness0speculation2Case5) {
  const int model_id = 0;
  const int staleness = 0;
  const int speculation = 2;
  ThreadsafeQueue<Message> reply_queue;
  std::unique_ptr<AbstractStorage> storage(new MapStorage<int>());
  std::unique_ptr<AbstractSparseSSPRecorder> recorder(
      new UnorderedMapSparseSSPRecorder(staleness, speculation));
  std::unique_ptr<AbstractModel> model(
      new SparseSSPModel(model_id, std::move(storage), std::move(recorder), &reply_queue, staleness, speculation));

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

  Message msg;
  // Get_2_0
  msg = CreateMessage(Flag::kGet, 0, 2, 0, 0, {0});
  model->Get(msg);
  // Get_3_0
  msg = CreateMessage(Flag::kGet, 0, 3, 0, 0, {1});
  model->Get(msg);

  reply_queue.WaitAndPop(&check_msg);
  EXPECT_EQ(check_msg.meta.flag, Flag::kGet);
  EXPECT_EQ(check_msg.meta.sender, 0);
  EXPECT_EQ(check_msg.meta.recver, 2);
  EXPECT_EQ(check_msg.meta.version, 0);
  EXPECT_EQ(check_msg.data.size(), 2);
  rep_keys = third_party::SArray<int>(check_msg.data[0]);
  rep_vals = third_party::SArray<int>(check_msg.data[1]);
  EXPECT_EQ(rep_keys.size(), 1);
  EXPECT_EQ(rep_vals.size(), 1);
  EXPECT_EQ(rep_keys[0], 0);
  EXPECT_EQ(rep_vals[0], 0);

  reply_queue.WaitAndPop(&check_msg);
  EXPECT_EQ(check_msg.meta.flag, Flag::kGet);
  EXPECT_EQ(check_msg.meta.version, 0);
  EXPECT_EQ(check_msg.meta.sender, 0);
  EXPECT_EQ(check_msg.meta.recver, 3);
  EXPECT_EQ(check_msg.data.size(), 2);
  rep_keys = third_party::SArray<int>(check_msg.data[0]);
  rep_vals = third_party::SArray<int>(check_msg.data[1]);
  EXPECT_EQ(rep_keys.size(), 1);
  EXPECT_EQ(rep_vals.size(), 1);
  EXPECT_EQ(rep_keys[0], 1);
  EXPECT_EQ(rep_vals[0], 0);

  // Get_2_1
  msg = CreateMessage(Flag::kGet, 0, 2, 0, 1, {0});
  model->Get(msg);
  // Get_3_1
  msg = CreateMessage(Flag::kGet, 0, 3, 0, 1, {2});
  model->Get(msg);

  // Get_2_2 and Clock
  msg = CreateMessage(Flag::kGet, 0, 2, 0, 2, {1});
  model->Get(msg);
  msg = CreateMessage(Flag::kClock, 0, 2, 0, 0);
  model->Clock(msg);

  EXPECT_EQ(reply_queue.Size(), 1);
  reply_queue.WaitAndPop(&check_msg);
  EXPECT_EQ(check_msg.meta.flag, Flag::kGet);
  EXPECT_EQ(check_msg.meta.sender, 0);
  EXPECT_EQ(check_msg.meta.recver, 2);
  EXPECT_EQ(check_msg.meta.version, 1);
  EXPECT_EQ(check_msg.data.size(), 2);
  rep_keys = third_party::SArray<int>(check_msg.data[0]);
  rep_vals = third_party::SArray<int>(check_msg.data[1]);
  EXPECT_EQ(rep_keys.size(), 1);
  EXPECT_EQ(rep_vals.size(), 1);
  EXPECT_EQ(rep_keys[0], 0);
  EXPECT_EQ(rep_vals[0], 0);

  // Get_2_3 and Clock
  msg = CreateMessage(Flag::kGet, 0, 2, 0, 3, {1});
  model->Get(msg);
  msg = CreateMessage(Flag::kClock, 0, 2, 0, 1);
  model->Clock(msg);

  EXPECT_EQ(reply_queue.Size(), 0);

  // Get_3_2 and Clock
  msg = CreateMessage(Flag::kGet, 0, 3, 0, 2, {1});
  model->Get(msg);
  msg = CreateMessage(Flag::kClock, 0, 3, 0, 0);
  model->Clock(msg);
  
  EXPECT_EQ(reply_queue.Size(), 2);
}

// staleness = 0
// speculation = 2 (Send the Get in the next 2 iterations before Clock)
// The two threads are accessing the different key.
// Get_i_j represents the jth Get from thread i
//
// Thread 2              Thread 3
// Get_2_0 [0](reply)      Get_3_0 [1](reply)
// Get_2_1 [0]             Get_3_1 [2]
//
// Get_2_2 [2]
// Clock (reply since Get_2_1 does not conflict with Get_3_0)
//
// Get_2_3 [2]
// Clock (No reply due to conflict between Get_2_2 and Get_3_1)
//
//                         Get_3_2 [1]
//                         Clock (Reply Get_3_1)
//
//                         Get_3_3 [1]
//                         Clock (Reply Get_3_2 and Get_2_2)
//
TEST_F(TestSparseSSPModel, staleness0speculation2Case6) {
  const int model_id = 0;
  const int staleness = 0;
  const int speculation = 2;
  ThreadsafeQueue<Message> reply_queue;
  std::unique_ptr<AbstractStorage> storage(new MapStorage<int>());
  std::unique_ptr<AbstractSparseSSPRecorder> recorder(
      new UnorderedMapSparseSSPRecorder(staleness, speculation));
  std::unique_ptr<AbstractModel> model(
      new SparseSSPModel(model_id, std::move(storage), std::move(recorder), &reply_queue, staleness, speculation));

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

  Message msg;
  // Get_2_0
  msg = CreateMessage(Flag::kGet, 0, 2, 0, 0, {0});
  model->Get(msg);
  // Get_3_0
  msg = CreateMessage(Flag::kGet, 0, 3, 0, 0, {1});
  model->Get(msg);

  reply_queue.WaitAndPop(&check_msg);
  EXPECT_EQ(check_msg.meta.flag, Flag::kGet);
  EXPECT_EQ(check_msg.meta.sender, 0);
  EXPECT_EQ(check_msg.meta.recver, 2);
  EXPECT_EQ(check_msg.meta.version, 0);
  EXPECT_EQ(check_msg.data.size(), 2);
  rep_keys = third_party::SArray<int>(check_msg.data[0]);
  rep_vals = third_party::SArray<int>(check_msg.data[1]);
  EXPECT_EQ(rep_keys.size(), 1);
  EXPECT_EQ(rep_vals.size(), 1);
  EXPECT_EQ(rep_keys[0], 0);
  EXPECT_EQ(rep_vals[0], 0);

  reply_queue.WaitAndPop(&check_msg);
  EXPECT_EQ(check_msg.meta.flag, Flag::kGet);
  EXPECT_EQ(check_msg.meta.version, 0);
  EXPECT_EQ(check_msg.meta.sender, 0);
  EXPECT_EQ(check_msg.meta.recver, 3);
  EXPECT_EQ(check_msg.data.size(), 2);
  rep_keys = third_party::SArray<int>(check_msg.data[0]);
  rep_vals = third_party::SArray<int>(check_msg.data[1]);
  EXPECT_EQ(rep_keys.size(), 1);
  EXPECT_EQ(rep_vals.size(), 1);
  EXPECT_EQ(rep_keys[0], 1);
  EXPECT_EQ(rep_vals[0], 0);

  // Get_2_1
  msg = CreateMessage(Flag::kGet, 0, 2, 0, 1, {0});
  model->Get(msg);
  // Get_3_1
  msg = CreateMessage(Flag::kGet, 0, 3, 0, 1, {2});
  model->Get(msg);

  // Get_2_2 and Clock
  msg = CreateMessage(Flag::kGet, 0, 2, 0, 2, {2});
  model->Get(msg);
  msg = CreateMessage(Flag::kClock, 0, 2, 0, 0);
  model->Clock(msg);

  EXPECT_EQ(reply_queue.Size(), 1);
  reply_queue.WaitAndPop(&check_msg);
  EXPECT_EQ(check_msg.meta.flag, Flag::kGet);
  EXPECT_EQ(check_msg.meta.sender, 0);
  EXPECT_EQ(check_msg.meta.recver, 2);
  EXPECT_EQ(check_msg.meta.version, 1);
  EXPECT_EQ(check_msg.data.size(), 2);
  rep_keys = third_party::SArray<int>(check_msg.data[0]);
  rep_vals = third_party::SArray<int>(check_msg.data[1]);
  EXPECT_EQ(rep_keys.size(), 1);
  EXPECT_EQ(rep_vals.size(), 1);
  EXPECT_EQ(rep_keys[0], 0);
  EXPECT_EQ(rep_vals[0], 0);

  // Get_2_3 and Clock
  msg = CreateMessage(Flag::kGet, 0, 2, 0, 3, {2});
  model->Get(msg);
  msg = CreateMessage(Flag::kClock, 0, 2, 0, 1);
  model->Clock(msg);

  EXPECT_EQ(reply_queue.Size(), 0);

  // Get_3_2 and Clock
  msg = CreateMessage(Flag::kGet, 0, 3, 0, 2, {1});
  model->Get(msg);
  msg = CreateMessage(Flag::kClock, 0, 3, 0, 0);
  model->Clock(msg);
  
  EXPECT_EQ(reply_queue.Size(), 1);
  reply_queue.WaitAndPop(&check_msg);

  // Get_3_3 and Clock
  msg = CreateMessage(Flag::kGet, 0, 3, 0, 3, {1});
  model->Get(msg);
  msg = CreateMessage(Flag::kClock, 0, 3, 0, 1);
  model->Clock(msg);
  
  EXPECT_EQ(reply_queue.Size(), 2);
}


// staleness = 0
// speculation = 2 (Send the Get in the next 2 iterations before Clock)
// The two threads are accessing the different key.
// Get_i_j represents the jth Get from thread i
//
// Thread 2              Thread 3
// Get_2_0 [0](reply)      Get_3_0 [1](reply)
// Get_2_1 [0]             Get_3_1 [2]
//
// Clock (reply since Get_2_1 does not conflict with Get_3_0)
//
// Get_2_3 [2]
// Clock (No Reply)
//
//                         Get_3_2 [1]
//                         Clock (Reply Get_3_1)
//
//                         Get_3_3 [1]
//                         Clock (Reply Get_3_2)
TEST_F(TestSparseSSPModel, ClockWithoutGet) {
  const int model_id = 0;
  const int staleness = 0;
  const int speculation = 2;
  ThreadsafeQueue<Message> reply_queue;
  std::unique_ptr<AbstractStorage> storage(new MapStorage<int>());
  std::unique_ptr<AbstractSparseSSPRecorder> recorder(
      new UnorderedMapSparseSSPRecorder(staleness, speculation));
  std::unique_ptr<AbstractModel> model(
      new SparseSSPModel(model_id, std::move(storage), std::move(recorder), &reply_queue, staleness, speculation));

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

  Message msg;
  // Get_2_0
  msg = CreateMessage(Flag::kGet, 0, 2, 0, 0, {0});
  model->Get(msg);
  // Get_3_0
  msg = CreateMessage(Flag::kGet, 0, 3, 0, 0, {1});
  model->Get(msg);

  reply_queue.WaitAndPop(&check_msg);
  EXPECT_EQ(check_msg.meta.flag, Flag::kGet);
  EXPECT_EQ(check_msg.meta.sender, 0);
  EXPECT_EQ(check_msg.meta.recver, 2);
  EXPECT_EQ(check_msg.meta.version, 0);
  EXPECT_EQ(check_msg.data.size(), 2);
  rep_keys = third_party::SArray<int>(check_msg.data[0]);
  rep_vals = third_party::SArray<int>(check_msg.data[1]);
  EXPECT_EQ(rep_keys.size(), 1);
  EXPECT_EQ(rep_vals.size(), 1);
  EXPECT_EQ(rep_keys[0], 0);
  EXPECT_EQ(rep_vals[0], 0);

  reply_queue.WaitAndPop(&check_msg);
  EXPECT_EQ(check_msg.meta.flag, Flag::kGet);
  EXPECT_EQ(check_msg.meta.version, 0);
  EXPECT_EQ(check_msg.meta.sender, 0);
  EXPECT_EQ(check_msg.meta.recver, 3);
  EXPECT_EQ(check_msg.data.size(), 2);
  rep_keys = third_party::SArray<int>(check_msg.data[0]);
  rep_vals = third_party::SArray<int>(check_msg.data[1]);
  EXPECT_EQ(rep_keys.size(), 1);
  EXPECT_EQ(rep_vals.size(), 1);
  EXPECT_EQ(rep_keys[0], 1);
  EXPECT_EQ(rep_vals[0], 0);

  // Get_2_1
  msg = CreateMessage(Flag::kGet, 0, 2, 0, 1, {0});
  model->Get(msg);
  // Get_3_1
  msg = CreateMessage(Flag::kGet, 0, 3, 0, 1, {2});
  model->Get(msg);

  // Clock from thread2
  msg = CreateMessage(Flag::kClock, 0, 2, 0, 0);
  model->Clock(msg);

  EXPECT_EQ(reply_queue.Size(), 1);
  reply_queue.WaitAndPop(&check_msg);
  EXPECT_EQ(check_msg.meta.flag, Flag::kGet);
  EXPECT_EQ(check_msg.meta.sender, 0);
  EXPECT_EQ(check_msg.meta.recver, 2);
  EXPECT_EQ(check_msg.meta.version, 1);
  EXPECT_EQ(check_msg.data.size(), 2);
  rep_keys = third_party::SArray<int>(check_msg.data[0]);
  rep_vals = third_party::SArray<int>(check_msg.data[1]);
  EXPECT_EQ(rep_keys.size(), 1);
  EXPECT_EQ(rep_vals.size(), 1);
  EXPECT_EQ(rep_keys[0], 0);
  EXPECT_EQ(rep_vals[0], 0);

  // Get_2_3 and Clock
  msg = CreateMessage(Flag::kGet, 0, 2, 0, 3, {2});
  model->Get(msg);
  msg = CreateMessage(Flag::kClock, 0, 2, 0, 1);
  model->Clock(msg);

  EXPECT_EQ(reply_queue.Size(), 0);

  // Get_3_2 and Clock
  msg = CreateMessage(Flag::kGet, 0, 3, 0, 2, {1});
  model->Get(msg);
  msg = CreateMessage(Flag::kClock, 0, 3, 0, 0);
  model->Clock(msg);
  
  EXPECT_EQ(reply_queue.Size(), 1);
  reply_queue.WaitAndPop(&check_msg);

  // Get_3_3 and Clock
  msg = CreateMessage(Flag::kGet, 0, 3, 0, 3, {1});
  model->Get(msg);
  msg = CreateMessage(Flag::kClock, 0, 3, 0, 1);
  model->Clock(msg);
  
  EXPECT_EQ(reply_queue.Size(), 1);
}

}  // namespace
}  // namespace flexps
