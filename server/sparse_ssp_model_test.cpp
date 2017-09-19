#include "glog/logging.h"
#include "gtest/gtest.h"

#include "base/threadsafe_queue.hpp"
#include "server/sparse_ssp_model.hpp"
#include "server/sparse_ssp_controller.hpp"

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

class MessageFactory {
public:
  static Message Create(Flag _flag, int _model_id, int _sender, int _recver, int _version, 
                        third_party::SArray<int> keys = {}, third_party::SArray<int> values = {}) {
    Message m;
    m.meta.flag = _flag;
    m.meta.model_id = _model_id;
    m.meta.sender = _sender;
    m.meta.recver = _recver;
    m.meta.version = _version;
 
    if (keys.size() != 0)
      m.AddData(keys);
    if (values.size() != 0)
      m.AddData(values);
    return m;
  }
};

TEST_F(TestSparseSSPModel, SArrayCasting) {
  third_party::SArray<uint32_t> test({0});
  auto hehe = third_party::SArray<char>(test);
  auto haha = third_party::SArray<uint32_t>(hehe);
  EXPECT_EQ(haha[0], 0);
}

TEST_F(TestSparseSSPModel, MessageFactory) {
  Message m1 = MessageFactory::Create(Flag::kAdd, 0, 3, 0, 0, {1}, {2});
  EXPECT_EQ(m1.data.size(), 2);
  auto rep_keys = third_party::SArray<int>(m1.data[0]);
  auto rep_vals = third_party::SArray<int>(m1.data[1]);
  EXPECT_EQ(m1.meta.flag, Flag::kAdd);
  EXPECT_EQ(rep_keys.size(), 1);
  EXPECT_EQ(rep_vals.size(), 1);
  EXPECT_EQ(rep_keys[0], 1);
  EXPECT_EQ(rep_vals[0], 2);

  Message m2 = MessageFactory::Create(Flag::kGet, 0, 3, 0, 0, {3});
  EXPECT_EQ(m2.data.size(), 1);
  rep_keys = third_party::SArray<int>(m2.data[0]);
  EXPECT_EQ(m2.meta.flag, Flag::kGet);
  EXPECT_EQ(rep_keys.size(), 1);
  EXPECT_EQ(rep_keys[0], 3);

  Message m3 = MessageFactory::Create(Flag::kClock, 0, 3, 0, 0);
  EXPECT_EQ(m3.data.size(), 0);
}

TEST_F(TestSparseSSPModel, CheckConstructor) {
  const int model_id = 0;
  const int staleness = 2;
  const int speculation = 2;
  ThreadsafeQueue<Message> reply_queue;

  std::unique_ptr<AbstractStorage> storage(new Storage<int>());
  std::unique_ptr<AbstractSparseSSPController> sparse_ssp_controller(
      new SparseSSPController(staleness, speculation, 
        std::unique_ptr<AbstractPendingBuffer>(new SparsePendingBuffer()),
        std::unique_ptr<AbstractConflictDetector>(new SparseConflictDetector())));
  std::unique_ptr<AbstractModel> model(
      new SparseSSPModel(model_id, std::move(storage), &reply_queue, std::move(sparse_ssp_controller)));
}

TEST_F(TestSparseSSPModel, GetAndAdd) {
  const int model_id = 0;
  const int staleness = 2;
  const int speculation = 2;
  ThreadsafeQueue<Message> reply_queue;
  std::unique_ptr<AbstractStorage> storage(new Storage<int>());
  std::unique_ptr<AbstractSparseSSPController> sparse_ssp_controller(
      new SparseSSPController(staleness, speculation, 
        std::unique_ptr<AbstractPendingBuffer>(new SparsePendingBuffer()),
        std::unique_ptr<AbstractConflictDetector>(new SparseConflictDetector())));
  std::unique_ptr<AbstractModel> model(
      new SparseSSPModel(model_id, std::move(storage), &reply_queue, std::move(sparse_ssp_controller)));

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
  Message m0 = MessageFactory::Create(Flag::kAdd, 0, 2, 0, 0, {0}, {0});
  model->Add(m0);

  // worker3 add 1 to param 1 with version 0
  Message m1 = MessageFactory::Create(Flag::kAdd, 0, 3, 0, 0, {1}, {1});
  model->Add(m1);

  // worker 2 get 0 from 0 with version 0
  Message m2 = MessageFactory::Create(Flag::kGet, 0, 2, 0, 0, {0});
  model->Get(m2);

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

  // Message 3
  Message m3 = MessageFactory::Create(Flag::kGet, 0, 3, 0, 0, {1});
  model->Get(m3);

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

  // Message 4
  Message m4 = MessageFactory::Create(Flag::kAdd, 0, 3, 0, 0, {1}, {1});
  model->Add(m4);

  // Message 5
  Message m5 = MessageFactory::Create(Flag::kGet, 0, 3, 0, 1, {1});
  model->Get(m5);

  // Message 6
  Message m6 = MessageFactory::Create(Flag::kGet, 0, 3, 0, 2, {1});
  model->Get(m6);

  // TODO(Ruoyu Wu): How to check no reply

  // Message 7
  Message m7 = MessageFactory::Create(Flag::kClock, 0, 3, 0, 0);
  model->Clock(m7);

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
  std::unique_ptr<AbstractSparseSSPController> sparse_ssp_controller(
      new SparseSSPController(staleness, speculation, 
        std::unique_ptr<AbstractPendingBuffer>(new SparsePendingBuffer()),
        std::unique_ptr<AbstractConflictDetector>(new SparseConflictDetector())));
  std::unique_ptr<AbstractModel> model(
      new SparseSSPModel(model_id, std::move(storage), &reply_queue, std::move(sparse_ssp_controller)));

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
  Message m1 = MessageFactory::Create(Flag::kAdd, 0, 2, 0, 0, {0}, {0});;
  model->Add(m1);

  // Message2
  Message m2 = MessageFactory::Create(Flag::kAdd, 0, 3, 0, 0, {1}, {1});;
  model->Add(m2);

  // Message3
  Message m3 = MessageFactory::Create(Flag::kGet, 0, 2, 0, 0, {0});
  model->Get(m3);
  reply_queue.WaitAndPop(&check_msg);

  // Message4
  Message m4 = MessageFactory::Create(Flag::kGet, 0, 3, 0, 0, {1});
  model->Get(m4);

  // Message5
  Message m5 = MessageFactory::Create(Flag::kGet, 0, 2, 0, 1, {0});;
  model->Get(m5);
  reply_queue.WaitAndPop(&check_msg);

  // Message6
  Message m6 = MessageFactory::Create(Flag::kGet, 0, 3, 0, 1, {1});;
  model->Get(m6);

  // Message 7
  Message m7 = MessageFactory::Create(Flag::kClock, 0, 3, 0, 0);;
  model.get()->Clock(m7);
  
  reply_queue.WaitAndPop(&check_msg);

  // Message 8
  Message m8 = MessageFactory::Create(Flag::kGet, 0, 3, 0, 2, {1});;
  model->Get(m8);

  // Message 9
  Message m9 = MessageFactory::Create(Flag::kClock, 0, 3, 0, 1);
  model.get()->Clock(m9);

  reply_queue.WaitAndPop(&check_msg);
  EXPECT_EQ(check_msg.meta.version, 2);

  // Message 10
  Message m10 = MessageFactory::Create(Flag::kGet, 0, 3, 0, 3, {1});;
  model->Get(m10);

  // Message11
  Message m11 = MessageFactory::Create(Flag::kGet, 0, 3, 0, 4, {1});;
  model->Get(m11);

  // Message 12
  Message m12 = MessageFactory::Create(Flag::kClock, 0, 3, 0, 2);;
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
  Message m13 = MessageFactory::Create(Flag::kClock, 0, 3, 0, 3);
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

}  // namespace
}  // namespace flexps
