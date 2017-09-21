#include "glog/logging.h"
#include "gtest/gtest.h"

#include "base/message.hpp"
#include "base/threadsafe_queue.hpp"
#include "server/asp_model.hpp"
#include "server/map_storage.hpp"

namespace flexps {
namespace {

class TestASPModel : public testing::Test {
 public:
  TestASPModel() {}
  ~TestASPModel() {}

 protected:
  void SetUp() {}
  void TearDown() {}
};

TEST_F(TestASPModel, CheckConstructor) {
  ThreadsafeQueue<Message> reply_queue;
  int model_id = 0;
  std::unique_ptr<AbstractStorage> storage(new MapStorage<int>());
  std::unique_ptr<AbstractModel> model(new ASPModel(model_id, std::move(storage), &reply_queue));
}

TEST_F(TestASPModel, CheckGetAndAdd) {
  ThreadsafeQueue<Message> reply_queue;
  int model_id = 0;
  std::unique_ptr<AbstractStorage> storage(new MapStorage<int>());
  std::unique_ptr<AbstractModel> model(new ASPModel(model_id, std::move(storage), &reply_queue));
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

  msg = CreateMessage(Flag::kGet, 0, 3, 0, 1, {1});
  model->Get(msg);

  msg = CreateMessage(Flag::kAdd, 0, 2, 0, 3, {1}, {1});
  model->Add(msg);

  EXPECT_EQ(reply_queue.Size(), 1);
  reply_queue.WaitAndPop(&check_msg);
  EXPECT_EQ(check_msg.meta.flag, Flag::kGet);
  EXPECT_EQ(check_msg.meta.sender, 0);
  EXPECT_EQ(check_msg.meta.recver, 3);
  EXPECT_EQ(check_msg.data.size(), 2);
  rep_keys = third_party::SArray<int>(check_msg.data[0]);
  rep_vals = third_party::SArray<int>(check_msg.data[1]);
  EXPECT_EQ(rep_keys.size(), 1);
  EXPECT_EQ(rep_vals.size(), 1);
  EXPECT_EQ(rep_keys[0], 1);
  EXPECT_EQ(rep_vals[0], 0);


  // Get_2_4 and Clock
  msg = CreateMessage(Flag::kGet, 0, 2, 0, 4, {1});
  model->Get(msg);

  EXPECT_EQ(reply_queue.Size(), 1);
  reply_queue.WaitAndPop(&check_msg);
  EXPECT_EQ(check_msg.meta.flag, Flag::kGet);
  EXPECT_EQ(check_msg.meta.sender, 0);
  EXPECT_EQ(check_msg.meta.recver, 2);
  EXPECT_EQ(check_msg.data.size(), 2);
  rep_keys = third_party::SArray<int>(check_msg.data[0]);
  rep_vals = third_party::SArray<int>(check_msg.data[1]);
  EXPECT_EQ(rep_keys.size(), 1);
  EXPECT_EQ(rep_vals.size(), 1);
  EXPECT_EQ(rep_keys[0], 1);
  EXPECT_EQ(rep_vals[0], 1);

  // Get_3_2 and Clock
  msg = CreateMessage(Flag::kAdd, 0, 3, 0, 2, {0}, {1});
  model->Add(msg);
  msg = CreateMessage(Flag::kGet, 0, 3, 0, 0, {0});
  model->Get(msg);

  EXPECT_EQ(reply_queue.Size(), 1);
  reply_queue.WaitAndPop(&check_msg);
  EXPECT_EQ(check_msg.meta.flag, Flag::kGet);
  EXPECT_EQ(check_msg.meta.sender, 0);
  EXPECT_EQ(check_msg.meta.recver, 3);
  EXPECT_EQ(check_msg.data.size(), 2);
  rep_keys = third_party::SArray<int>(check_msg.data[0]);
  rep_vals = third_party::SArray<int>(check_msg.data[1]);
  EXPECT_EQ(rep_keys.size(), 1);
  EXPECT_EQ(rep_vals.size(), 1);
  EXPECT_EQ(rep_keys[0], 0);
  EXPECT_EQ(rep_vals[0], 1);
}

}  // namespace
}  // namespace flexps
