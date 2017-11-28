#include "gtest/gtest.h"
#include "glog/logging.h"

#include "driver/engine.hpp"
#include "worker/kv_client_table.hpp"
#include "worker/kv_chunk_client_table.hpp"
#include "worker/simple_kv_table.hpp"
#include "worker/simple_kv_chunk_table.hpp"

namespace flexps {
namespace {

/*
 * Test for Engine which depends on
 *  Mailbox
 *  SimpleIdMapper
 *  KVEngine
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

TEST_F(TestEngine, SimpleTaskMapStorage) {
  Node node{0, "localhost", 12353};
  Engine engine(node, {node});
  // start
  engine.StartEverything();

  engine.CreateTable<float>(0, {{0, 10}}, 
      ModelType::SSP, StorageType::Map);  // table 0, range [0,10)
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

TEST_F(TestEngine, SimpleTaskVectorStorage) {
  Node node{0, "localhost", 12353};
  Engine engine(node, {node});
  // start
  engine.StartEverything();

  engine.CreateTable<float>(0, {{0, 10}}, 
      ModelType::SSP, StorageType::Vector);  // table 0, range [0,10)
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

      engine.CreateTable<float>(0, {{0, 10}, {10, 20}, {20, 30}},
          ModelType::SSP, StorageType::Map);  // table 0, range [0,10), [10, 20), [20, 30)
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

TEST_F(TestEngine, KVClientTableMapStorage) {
  Node node{0, "localhost", 12353};
  Engine engine(node, {node});
  // start
  engine.StartEverything();

  const int kTableId = 0;
  engine.CreateTable<float>(kTableId, {{0, 10}},
      ModelType::SSP, StorageType::Map);  // table 0, range [0,10)
  engine.Barrier();
  MLTask task;
  task.SetWorkerAlloc({{0, 3}});  // 3 workers on node 0
  task.SetTables({kTableId});  // Use table 0
  task.SetLambda([kTableId](const Info& info){
    LOG(INFO) << "Hi";
    LOG(INFO) << info.DebugString();
    ASSERT_TRUE(info.partition_manager_map.find(kTableId) != info.partition_manager_map.end());
    KVClientTable<float> table(info.thread_id, kTableId, info.send_queue, info.partition_manager_map.find(kTableId)->second, info.callback_runner);
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

TEST_F(TestEngine, KVClientTableVectorStorage) {
  Node node{0, "localhost", 12353};
  Engine engine(node, {node});
  // start
  engine.StartEverything();

  const int kTableId = 0;
  engine.CreateTable<float>(kTableId, {{0, 10}},
      ModelType::SSP, StorageType::Vector);  // table 0, range [0,10)
  engine.Barrier();
  MLTask task;
  task.SetWorkerAlloc({{0, 3}});  // 3 workers on node 0
  task.SetTables({kTableId});  // Use table 0
  task.SetLambda([kTableId](const Info& info){
    LOG(INFO) << "Hi";
    LOG(INFO) << info.DebugString();
    ASSERT_TRUE(info.partition_manager_map.find(kTableId) != info.partition_manager_map.end());
    KVClientTable<float> table(info.thread_id, kTableId, info.send_queue, info.partition_manager_map.find(kTableId)->second, info.callback_runner);
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

TEST_F(TestEngine, SimpleKVTableMapStorage) {
  Node node{0, "localhost", 12353};
  Engine engine(node, {node});
  // start
  engine.StartEverything();

  const int kTableId = 0;
  engine.CreateTable<float>(kTableId, {{0, 10}},
      ModelType::SSP, StorageType::Map);  // table 0, range [0,10)
  engine.Barrier();
  MLTask task;
  task.SetWorkerAlloc({{0, 3}});  // 3 workers on node 0
  task.SetTables({kTableId});  // Use table 0
  task.SetLambda([kTableId](const Info& info){
    LOG(INFO) << "Hi";
    LOG(INFO) << info.DebugString();
    ASSERT_TRUE(info.partition_manager_map.find(kTableId) != info.partition_manager_map.end());
    SimpleKVTable<float> table(info.thread_id, kTableId, info.send_queue, info.partition_manager_map.find(kTableId)->second, info.mailbox);
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

TEST_F(TestEngine, SimpleKVTableVectorStorage) {
  Node node{0, "localhost", 12353};
  Engine engine(node, {node});
  // start
  engine.StartEverything();

  const int kTableId = 0;
  engine.CreateTable<float>(kTableId, {{0, 10}},
      ModelType::SSP, StorageType::Vector);  // table 0, range [0,10)
  engine.Barrier();
  MLTask task;
  task.SetWorkerAlloc({{0, 3}});  // 3 workers on node 0
  task.SetTables({kTableId});  // Use table 0
  task.SetLambda([kTableId](const Info& info){
    LOG(INFO) << "Hi";
    LOG(INFO) << info.DebugString();
    ASSERT_TRUE(info.partition_manager_map.find(kTableId) != info.partition_manager_map.end());
    SimpleKVTable<float> table(info.thread_id, kTableId, info.send_queue, info.partition_manager_map.find(kTableId)->second, info.mailbox);
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

TEST_F(TestEngine, KVClientChunkTableVectorStorage) {
  Node node{0, "localhost", 12353};
  Engine engine(node, {node});
  // start
  engine.StartEverything();

  const int kTableId = 0;
  const int kStaleness = 0;
  const int kChunkSize = 2;
  engine.CreateTable<float>(kTableId, {{0, 10}},
      ModelType::SSP, StorageType::Vector, kStaleness, kChunkSize);  // table 0, range [0,10)
  engine.Barrier();
  MLTask task;
  task.SetWorkerAlloc({{0, 3}});  // 3 workers on node 0
  task.SetTables({kTableId});  // Use table 0
  task.SetLambda([kTableId](const Info& info){
    LOG(INFO) << "Hi";
    LOG(INFO) << info.DebugString();
    ASSERT_TRUE(info.partition_manager_map.find(kTableId) != info.partition_manager_map.end());
    KVChunkClientTable<float> table(info.thread_id, kTableId, info.send_queue, info.partition_manager_map.find(kTableId)->second, info.callback_runner);
    for (int i = 0; i < 5; ++ i) {
      std::vector<Key> keys{1};
      std::vector<std::vector<float>> vals{{0.5, 0.3}};
      table.AddChunk(keys, vals);
      std::vector<std::vector<float>*> ret(1);
      table.GetChunk(keys, ret);
      EXPECT_EQ(ret.size(), 1);
      LOG(INFO) << (*ret[0])[0] << (*ret[0])[1];
    }
  });
  engine.Run(task);

  // stop
  engine.StopEverything();
}

TEST_F(TestEngine, KVClientChunkTableMapStorage) {
  Node node{0, "localhost", 12353};
  Engine engine(node, {node});
  // start
  engine.StartEverything();

  const int kTableId = 0;
  const int kStaleness = 0;
  const int kChunkSize = 2;
  engine.CreateTable<float>(kTableId, {{0, 10}},
      ModelType::SSP, StorageType::Map, kStaleness, kChunkSize);  // table 0, range [0,10)
  engine.Barrier();
  MLTask task;
  task.SetWorkerAlloc({{0, 3}});  // 3 workers on node 0
  task.SetTables({kTableId});  // Use table 0
  task.SetLambda([kTableId](const Info& info){
    LOG(INFO) << "Hi";
    LOG(INFO) << info.DebugString();
    ASSERT_TRUE(info.partition_manager_map.find(kTableId) != info.partition_manager_map.end());
    KVChunkClientTable<float> table(info.thread_id, kTableId, info.send_queue, info.partition_manager_map.find(kTableId)->second, info.callback_runner);
    for (int i = 0; i < 5; ++ i) {
      std::vector<Key> keys{1};
      std::vector<std::vector<float>> vals{{0.5, 0.3}};
      table.AddChunk(keys, vals);
      std::vector<std::vector<float>*> ret(1);
      table.GetChunk(keys, ret);
      EXPECT_EQ(ret.size(), 1);
      LOG(INFO) << (*ret[0])[0] << (*ret[0])[1];
    }
  });
  engine.Run(task);

  // stop
  engine.StopEverything();
}

TEST_F(TestEngine, SimpleKVChunkTableMapStorage) {
  Node node{0, "localhost", 12353};
  Engine engine(node, {node});
  // start
  engine.StartEverything();

  const int kTableId = 0;
  const int kStaleness = 0;
  const int kChunkSize = 2;
  engine.CreateTable<float>(kTableId, {{0, 10}},
      ModelType::SSP, StorageType::Map, kStaleness, kChunkSize);  // table 0, range [0,10)
  engine.Barrier();
  MLTask task;
  task.SetWorkerAlloc({{0, 3}});  // 3 workers on node 0
  task.SetTables({kTableId});  // Use table 0
  task.SetLambda([kTableId](const Info& info){
    LOG(INFO) << "Hi";
    LOG(INFO) << info.DebugString();
    ASSERT_TRUE(info.partition_manager_map.find(kTableId) != info.partition_manager_map.end());
    SimpleKVChunkTable<float> table(info.thread_id, kTableId, info.send_queue, info.partition_manager_map.find(kTableId)->second, info.mailbox);
    for (int i = 0; i < 5; ++ i) {
      std::vector<Key> keys{1};
      std::vector<std::vector<float>> vals{{0.5, 0.3}};
      table.AddChunk(keys, vals);
      std::vector<std::vector<float>*> ret(1);
      table.GetChunk(keys, ret);
      EXPECT_EQ(ret.size(), 1);
      LOG(INFO) << (*ret[0])[0] << (*ret[0])[1];
    }
  });
  engine.Run(task);

  // stop
  engine.StopEverything();
}

TEST_F(TestEngine, SimpleKVChunkTableVectorStorage) {
  Node node{0, "localhost", 12353};
  Engine engine(node, {node});
  // start
  engine.StartEverything();

  const int kTableId = 0;
  const int kStaleness = 0;
  const int kChunkSize = 2;
  engine.CreateTable<float>(kTableId, {{0, 10}},
      ModelType::SSP, StorageType::Vector, kStaleness, kChunkSize);  // table 0, range [0,10)
  engine.Barrier();
  MLTask task;
  task.SetWorkerAlloc({{0, 3}});  // 3 workers on node 0
  task.SetTables({kTableId});  // Use table 0
  task.SetLambda([kTableId](const Info& info){
    LOG(INFO) << "Hi";
    LOG(INFO) << info.DebugString();
    ASSERT_TRUE(info.partition_manager_map.find(kTableId) != info.partition_manager_map.end());
    SimpleKVChunkTable<float> table(info.thread_id, kTableId, info.send_queue, info.partition_manager_map.find(kTableId)->second, info.mailbox);
    for (int i = 0; i < 5; ++ i) {
      std::vector<Key> keys{1};
      std::vector<std::vector<float>> vals{{0.5, 0.3}};
      table.AddChunk(keys, vals);
      std::vector<std::vector<float>*> ret(1);
      table.GetChunk(keys, ret);
      EXPECT_EQ(ret.size(), 1);
      LOG(INFO) << (*ret[0])[0] << (*ret[0])[1];
    }
  });
  engine.Run(task);

  // stop
  engine.StopEverything();
}

}  // namespace
}  // namespace flexps
