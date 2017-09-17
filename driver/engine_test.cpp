#include "gtest/gtest.h"
#include "glog/logging.h"

#include "driver/engine.hpp"
#include "worker/kv_client_table.hpp"

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

class FakeIdMapper : public AbstractIdMapper {
 public:
  virtual uint32_t GetNodeIdForThread(uint32_t tid) override {
    return tid;
  }
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

TEST_F(TestEngine, StartEverything) {
  Node node{0, "localhost", 12353};
  Engine engine(node, {node});
  engine.StartEverything();
  engine.StopEverything();
}

TEST_F(TestEngine, MultipleStartEverything) {
  std::vector<Node> nodes{
    {0, "localhost", 12353},
    {1, "localhost", 12354},
    {2, "localhost", 12355}};

  std::vector<std::thread> threads(nodes.size());
  for (int i = 0; i < nodes.size(); ++ i) {
    threads[i] = std::thread([&nodes, i]() {
      Engine engine(nodes[i], nodes);
      engine.StartEverything();
      engine.StopEverything();
    });
  }
  for (auto& th : threads) {
    th.join();
  }
}

TEST_F(TestEngine, SimpleTask) {
  Node node{0, "localhost", 12353};
  Engine engine(node, {node});
  // start
  engine.StartEverything();

  engine.CreateTable(0, {{0, 10}});  // table 0, range [0,10)
  engine.Barrier();
  MLTask task;
  task.SetWorkerAlloc({{0, 3}});  // 3 workers on node 0
  task.SetTables({0});  // Use table 0
  task.SetLambda([](const Info& info){
    LOG(INFO) << "Hi";
  });
  engine.Run(task);

  // stop
  engine.StopEverything();
}

TEST_F(TestEngine, MultipleTasks) {
  std::vector<Node> nodes{
    {0, "localhost", 12353},
    {1, "localhost", 12354},
    {2, "localhost", 12355}};

  std::vector<std::thread> threads(nodes.size());
  for (int i = 0; i < nodes.size(); ++ i) {
    threads[i] = std::thread([&nodes, i]() {
      Engine engine(nodes[i], nodes);
      // start
      engine.StartEverything();

      engine.CreateTable(0, {{0, 10}, {10, 20}, {20, 30}});  // table 0, range [0,10), [10, 20), [20, 30)
      engine.Barrier();
      MLTask task;
      // 3 workers on node 0, 2 workers on node 1, 3 workers on node 2
      task.SetWorkerAlloc({{0, 3}, {1, 2}, {2, 3}});
      task.SetTables({0});  // Use table 0
      task.SetLambda([](const Info& info){
        LOG(INFO) << "Hi";
      });
      engine.Run(task);

      // stop
      engine.StopEverything();
    });
  }
  for (auto& th : threads) {
    th.join();
  }
}

TEST_F(TestEngine, KVClientTable) {
  Node node{0, "localhost", 12353};
  Engine engine(node, {node});
  // start
  engine.StartEverything();

  const int kTableId = 0;
  engine.CreateTable(kTableId, {{0, 10}});  // table 0, range [0,10)
  engine.Barrier();
  MLTask task;
  task.SetWorkerAlloc({{0, 3}});  // 3 workers on node 0
  task.SetTables({kTableId});  // Use table 0
  task.SetLambda([kTableId](const Info& info){
    LOG(INFO) << "Hi";
    LOG(INFO) << info.DebugString();
    ASSERT_TRUE(info.range_manager_map.find(kTableId) != info.range_manager_map.end());
    KVClientTable<float> table(info.thread_id, kTableId, info.send_queue, &info.range_manager_map.find(kTableId)->second, info.callback_runner);
    for (int i = 0; i < 5; ++ i) {
      std::vector<Key> keys{1};
      std::vector<float> vals{0.5};
      table.Add(keys, vals);
      std::vector<float> ret;
      table.Get(keys, &ret);
      EXPECT_EQ(ret.size(), 1);
      LOG(INFO) << ret[0];
    }
  });
  engine.Run(task);

  // stop
  engine.StopEverything();
}

}  // namespace
}  // namespace flexps
