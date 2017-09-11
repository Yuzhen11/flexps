#include "glog/logging.h"
#include "gtest/gtest.h"

#include "server/ssp_model.hpp"
#include "base/threadsafe_queue.hpp"

namespace flexps {
namespace {

class TestSSPModel : public testing::Test {
 public:
  TestSSPModel() {}
  ~TestSSPModel() {}

 protected:
  void SetUp() {}
  void TearDown() {}
};

TEST_F(TestSSPModel, CheckConstructor) {
  ThreadsafeQueue<Message> threadsafe_queue;
  int staleness = 1;
  std::vector<int> tids{2, 3};
  int model_id = 0;
  std::unique_ptr<AbstractStorage> storage(new Storage<int>());
  std::unique_ptr<AbstractModel> model(new SSPModel(model_id, tids, std::move(storage), staleness, &threadsafe_queue));
}

TEST_F(TestSSPModel, CheckGetAndAdd) {
  ThreadsafeQueue<Message> threadsafe_queue;
  int staleness = 1;
  std::vector<int> tids{2, 3};
  int model_id = 0;
  std::unique_ptr<AbstractStorage> storage(new Storage<int>());
  std::unique_ptr<AbstractModel> model(new SSPModel(model_id, tids, std::move(storage), staleness, &threadsafe_queue));

  // Message1
  Message m1;
  m1.meta.flag = Flag::kGet;
  m1.meta.model_id = 0;
  m1.meta.sender = 2;
  m1.meta.recver = 0;
  m1.bin << 1 << 0;

  // Message2
  Message m2;
  m2.meta.flag = Flag::kGet;
  m2.meta.model_id = 0;
  m2.meta.sender = 3;
  m2.meta.recver = 0;
  m2.bin << 1 << 1;

  // Message3
  Message m3;
  m3.meta.flag = Flag::kAdd;
  m3.meta.model_id = 0;
  m3.meta.sender = 2;
  m3.meta.recver = 0;
  m3.bin << 1 << 0 << 1;

  // Message4
  Message m4;
  m4.meta.flag = Flag::kAdd;
  m4.meta.model_id = 0;
  m4.meta.sender = 3;
  m4.meta.recver = 0;
  m4.bin << 1 << 1 << 2;

  // Message5
  Message m5;
  m5.meta.flag = Flag::kGet;
  m5.meta.model_id = 0;
  m5.meta.sender = 2;
  m5.meta.recver = 0;
  m5.bin << 1 << 0;

  // Message6
  Message m6;
  m6.meta.flag = Flag::kGet;
  m6.meta.model_id = 0;
  m6.meta.sender = 3;
  m6.meta.recver = 0;
  m6.bin << 1 << 1;

  model.get()->Get(m1);
  model.get()->Get(m2);
  model.get()->Add(m3);
  model.get()->Add(m4);
  model.get()->Get(m5);
  model.get()->Get(m6);

  // Check
  Message check_message;
  int key = 1996;
  int check_number = 223;

  EXPECT_EQ(threadsafe_queue.size(), 4);

  threadsafe_queue.WaitAndPop(&check_message);
  check_message.bin >> key >> check_number;
  EXPECT_EQ(key, 0);
  EXPECT_EQ(check_number, 0);

  threadsafe_queue.WaitAndPop(&check_message);
  check_message.bin >> key >> check_number;
  EXPECT_EQ(key, 1);
  EXPECT_EQ(check_number, 0);

  threadsafe_queue.WaitAndPop(&check_message);
  check_message.bin >> key >> check_number;
  EXPECT_EQ(key, 0);
  EXPECT_EQ(check_number, 1);

  threadsafe_queue.WaitAndPop(&check_message);
  check_message.bin >> key >> check_number;
  EXPECT_EQ(key, 1);
  EXPECT_EQ(check_number, 2);
}

TEST_F(TestSSPModel, CheckClock) {
  ThreadsafeQueue<Message> threadsafe_queue;
  int staleness = 1;
  std::vector<int> tids{2, 3};
  int model_id = 0;
  std::unique_ptr<AbstractStorage> storage(new Storage<int>());
  std::unique_ptr<AbstractModel> model(new SSPModel(model_id, tids, std::move(storage), staleness, &threadsafe_queue));

  //model.get()->Clock();
}

TEST_F(TestSSPModel, CheckStaleness) {
  /* There are two servers(0, 1), two clients(2, 3) and four parameters(0, 1, 2, 3)
   * param(0, 2) will be stored in server(0) and param(1, 3) will be stored in server(1)
   * WORK for client(2):
   *   get param(0, 1)
   *   add param(0, 1)
   *   clock
   *   get param(1, 3)
   *   add param(1, 3)
   *   clock
   *   get param(0)
   *   add param(0)
   *   clock
   *   exit
   * WORK for client(3):
   *   get param(2, 3)
   *   add param(2, 3)
   *   clock
   *   exit
   */

}

}  // namespace
}  // namespace flexps
