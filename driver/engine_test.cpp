#include "gtest/gtest.h"
#include "glog/logging.h"

#include "driver/engine.hpp"

namespace flexps {
namespace {

/*
 * Test for Engine which depends on
 *  Mailbox
 *  WorkerHelperThread
 *  ServerThread
 *  AppBlocker
 *  ...
 *
 *  The failure of the test may be caused by each of these components.
 */
class TestEngine: public testing::Test {
 public:
  TestEngine() {}
  ~TestEngine() {}

 protected:
  void SetUp() {}
  void TearDown() {}
};

TEST_F(TestEngine, Construct) {
  Node node{0, "localhost", 12353};
  Engine engine(node, {node});
}

TEST_F(TestEngine, StartMailbox) {
  Node node{0, "localhost", 12353};
  Engine engine(node, {node});
  engine.CreateMailbox();
  engine.StartMailbox();
  engine.StopMailbox();
}

TEST_F(TestEngine, StartAll) {
  Node node{0, "localhost", 12352};
  Engine engine(node, {node});
  // start
  engine.CreateMailbox();
  engine.StartSender();
  engine.StartServerThreads();
  engine.StartWorkerHelperThreads();
  engine.StartMailbox();

  // stop
  engine.StopSender();
  engine.StopMailbox();
  engine.StopServerThreads();
  engine.StopWorkerHelperThreads();
}

}  // namespace
}  // namespace flexps
