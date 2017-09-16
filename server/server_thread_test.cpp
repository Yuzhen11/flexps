#include "glog/logging.h"
#include "gtest/gtest.h"

#include "base/magic.hpp"
#include "server/abstract_model.hpp"
#include "server/server_thread.hpp"
#include "server/ssp_model.hpp"

namespace flexps {
namespace {

class TestServerThread : public testing::Test {
 public:
  TestServerThread() {}
  ~TestServerThread() {}

 protected:
  void SetUp() {}
  void TearDown() {}
};

class FakeModel : public AbstractModel {
 public:
  virtual void Clock(Message&) override { clock_count_ += 1; }
  virtual void Add(Message&) override { add_count_ += 1; }
  virtual void Get(Message&) override { get_count_ += 1; }
  virtual int GetProgress(int tid) override { return -1; }
  virtual void ResetWorker(Message& msg) override { }

  int clock_count_ = 0;
  int add_count_ = 0;
  int get_count_ = 0;
};

TEST_F(TestServerThread, Construct) { ServerThread server_thread(0); }

TEST_F(TestServerThread, Basic) {
  ServerThread server_thread(0);
  std::unique_ptr<AbstractModel> model(new FakeModel());
  const uint32_t model_id = 0;
  server_thread.RegisterModel(model_id, std::move(model));
  auto* p = static_cast<FakeModel*>(server_thread.GetModel(model_id));
  server_thread.Start();

  auto* work_queue = server_thread.GetWorkQueue();
  Message m;
  m.meta.flag = Flag::kClock;
  m.meta.model_id = model_id;
  work_queue->Push(m);
  work_queue->Push(m);

  Message m2;
  m2.meta.flag = Flag::kAdd;
  m2.meta.model_id = model_id;
  work_queue->Push(m2);

  Message m3;
  m3.meta.flag = Flag::kGet;
  m3.meta.model_id = model_id;
  work_queue->Push(m3);
  work_queue->Push(m3);
  work_queue->Push(m3);

  Message exit_msg;
  exit_msg.meta.flag = Flag::kExit;
  work_queue->Push(exit_msg);
  server_thread.Stop();

  EXPECT_EQ(p->clock_count_, 2);
  EXPECT_EQ(p->add_count_, 1);
  EXPECT_EQ(p->get_count_, 3);
}

}  // namespace
}  // namespace flexps
