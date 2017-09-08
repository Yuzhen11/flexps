#include "gtest/gtest.h"

#include "glog/logging.h"
#include <thread>
#include <future>

#include "app_blocker.hpp"


namespace flexps {
namespace {

class TestAppBlocker : public testing::Test {
 public:
  TestAppBlocker() {}
  ~TestAppBlocker() {}

 protected:
  void SetUp() {}
  void TearDown() {}
};

TEST_F(TestAppBlocker, Construct) { AppBlocker blocker; }


TEST_F(TestAppBlocker, Register) {
  AppBlocker blocker;
  int f1_counter = 0;
  int f2_counter = 0;
  Message m;
  m.meta.sender = 237;  // set the sender to check whether the messages are the same 
  auto f1 = [&f1_counter, m](Message& message) {
    f1_counter += 1;
    EXPECT_EQ(m.meta.sender, message.meta.sender);
  };
  auto f2 = [&f2_counter, m](Message& message) {
    f2_counter += 1;
    EXPECT_EQ(m.meta.sender, message.meta.sender);
  };
  blocker.RegisterRecvHandle(0, 0, f1);
  blocker.RegisterRecvFinishHandle(0, 0, f2);
  blocker.NewRequest(0, 0, 2);
  blocker.AddResponse(0, 0, m);
  EXPECT_EQ(f1_counter, 1);
  EXPECT_EQ(f2_counter, 0);
  blocker.AddResponse(0, 0, m);
  EXPECT_EQ(f1_counter, 2);
  EXPECT_EQ(f2_counter, 1);
}

TEST_F(TestAppBlocker, MultiThreads) {
  AppBlocker blocker;
  int f1_counter = 0;
  int f2_counter = 0;
  Message m;
  m.meta.sender = 237;  // set the sender to check whether the messages are the same 
  auto f1 = [&f1_counter, m](Message& message) {
    f1_counter += 1;
    EXPECT_EQ(m.meta.sender, message.meta.sender);
  };
  auto f2 = [&f2_counter, m](Message& message) {
    f2_counter += 1;
    EXPECT_EQ(m.meta.sender, message.meta.sender);
  };
  blocker.RegisterRecvHandle(0, 0, f1);
  blocker.RegisterRecvFinishHandle(0, 0, f2);
  std::promise<void> prom;
  std::future<void> fut = prom.get_future();
  std::thread th([&blocker, &prom] {
    blocker.NewRequest(0, 0, 2);
    prom.set_value();
    blocker.WaitRequest(0,0);
  });
  fut.get();
  blocker.AddResponse(0, 0, m);
  EXPECT_EQ(f1_counter, 1);
  EXPECT_EQ(f2_counter, 0);
  blocker.AddResponse(0, 0, m);
  EXPECT_EQ(f1_counter, 2);
  EXPECT_EQ(f2_counter, 1);
  th.join();
}

TEST_F(TestAppBlocker, MultiThreadsReused) {
  AppBlocker blocker;
  int f1_counter = 0;
  int f2_counter = 0;
  Message m;
  m.meta.sender = 237;  // set the sender to check whether the messages are the same 
  auto f1 = [&f1_counter, m](Message& message) {
    f1_counter += 1;
    EXPECT_EQ(m.meta.sender, message.meta.sender);
  };
  auto f2 = [&f2_counter, m](Message& message) {
    f2_counter += 1;
    EXPECT_EQ(m.meta.sender, message.meta.sender);
  };
  blocker.RegisterRecvHandle(0, 0, f1);
  blocker.RegisterRecvFinishHandle(0, 0, f2);
  {
    std::promise<void> prom;
    std::future<void> fut = prom.get_future();
    std::thread th([&blocker, &prom] {
      blocker.NewRequest(0, 0, 2);
      prom.set_value();
      blocker.WaitRequest(0,0);
    });
    fut.get();
    blocker.AddResponse(0, 0, m);
    EXPECT_EQ(f1_counter, 1);
    EXPECT_EQ(f2_counter, 0);
    blocker.AddResponse(0, 0, m);
    EXPECT_EQ(f1_counter, 2);
    EXPECT_EQ(f2_counter, 1);
    th.join();
  }
  // Second round
  {
    f1_counter = 0;
    f2_counter = 0;
    std::promise<void> prom;
    std::future<void> fut = prom.get_future();
    std::thread th([&blocker, &prom] {
      blocker.NewRequest(0, 0, 2);
      prom.set_value();
      blocker.WaitRequest(0,0);
    });
    fut.get();
    blocker.AddResponse(0, 0, m);
    EXPECT_EQ(f1_counter, 1);
    EXPECT_EQ(f2_counter, 0);
    blocker.AddResponse(0, 0, m);
    EXPECT_EQ(f1_counter, 2);
    EXPECT_EQ(f2_counter, 1);
    th.join();
  }
}

}  // namespace
}  // namespace flexps
