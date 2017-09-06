#include "gtest/gtest.h"

#include "glog/logging.h"

#include "base/magic.hpp"
#include "server/abstract_model.hpp"
#include "server/server_thread.hpp"

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
  virtual void Clock(uint32_t tid) override { clock_count_ += 1; }
  virtual void Add(uint32_t tid, const Message&) override { add_count_ += 1; }
  virtual void Get(uint32_t tid, const Message&) override { get_count_ += 1; }

  int clock_count_ = 0;
  int add_count_ = 0;
  int get_count_ = 0;
};

TEST_F(TestServerThread, Construct) { ServerThread th; }

TEST_F(TestServerThread, Basic) {
  ServerThread th;
  std::unique_ptr<AbstractModel> model(new FakeModel());
  const uint32_t model_id = 0;
  th.RegisterModel(model_id, std::move(model));
  auto* p = static_cast<FakeModel*>(th.GetModel(model_id));
  th.Start();

  auto* work_queue = th.GetWorkQueue();
  uint32_t tid = 123;
  Message m;
  m.bin << kClock << tid << model_id;
  work_queue->Push(m);
  work_queue->Push(m);
  Message m2;
  m2.bin << kAdd << tid << model_id;
  work_queue->Push(m2);
  Message m3;
  m3.bin << kGet << tid << model_id;
  work_queue->Push(m3);
  work_queue->Push(m3);
  work_queue->Push(m3);

  Message m4;
  m4.bin << kExit;
  work_queue->Push(m4);

  th.Stop();

  EXPECT_EQ(p->clock_count_, 2);
  EXPECT_EQ(p->add_count_, 1);
  EXPECT_EQ(p->get_count_, 3);
}

}  // namespace
}  // namespace flexps
