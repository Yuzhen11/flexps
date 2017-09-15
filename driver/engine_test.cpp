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

TEST_F(TestEngine, CreateTable) {
  Node node{0, "localhost", 12351};
  Engine engine(node, {node});
  // start
  engine.CreateMailbox();
  engine.StartSender();
  engine.StartServerThreads();
  engine.StartWorkerHelperThreads();
  engine.StartMailbox();

  WorkerSpec worker_spec({{0, 3}});  // 3 workers on node 0
  engine.AllocateWorkers(&worker_spec);
  MLTask task;
  task.SetWorkerSpec(worker_spec);
  task.SetLambda([](const Info& info){
    LOG(INFO) << "Hi";
  });
  auto worker_threads = worker_spec.GetThreadIds();
  engine.CreateTable(0, {{0, 10}}, worker_threads);  // table 0, range [0,10)
  engine.Barrier();
  engine.Run(task);

  // stop
  engine.StopSender();
  engine.StopMailbox();
  engine.StopServerThreads();
  engine.StopWorkerHelperThreads();
}

}  // namespace
}  // namespace flexps
