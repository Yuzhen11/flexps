// #include "driver/engine.hpp"

#include "gflags/gflags.h"
#include "glog/logging.h"

#include "driver/engine.hpp"
#include "worker/kv_client_table.hpp"

#include <algorithm>
#include <numeric>

DEFINE_int32(my_id, -1, "The process id of this program");
DEFINE_string(config_file, "", "The config file path");

namespace flexps {

void Run() {
  CHECK_NE(FLAGS_my_id, -1);
  CHECK(!FLAGS_config_file.empty());
  VLOG(1) << FLAGS_my_id << " " << FLAGS_config_file;

  // 0. Parse config_file
  std::vector<Node> nodes = ParseFile(FLAGS_config_file);
  CHECK(CheckValidNodeIds(nodes));
  CHECK(CheckUniquePort(nodes));
  CHECK(CheckConsecutiveIds(nodes));
  Node my_node = GetNodeById(nodes, FLAGS_my_id);
  LOG(INFO) << my_node.DebugString();

  // 1. Start engine
  Engine engine(my_node, nodes);
  engine.StartEverything();

  // 2. Create tables
  const int kTableId = 0;
  const int kMaxKey = 1000;
  const int kStaleness = 1;
  std::vector<third_party::Range> range;
  for (int i = 0; i < nodes.size() - 1; ++ i) {
    range.push_back({kMaxKey / nodes.size() * i, kMaxKey / nodes.size() * (i + 1)});
  }
  range.push_back({kMaxKey / nodes.size() * (nodes.size() - 1), kMaxKey});
  engine.CreateTable<float>(kTableId, range, 
      ModelType::SSP, StorageType::Map, kStaleness);
  engine.Barrier();

  // 3. Construct tasks
  MLTask task;
  std::vector<WorkerAlloc> worker_alloc;
  for (auto& node : nodes) {
    worker_alloc.push_back({node.id, 10});  // each node has 10 workers
  }
  task.SetWorkerAlloc(worker_alloc);
  task.SetTables({kTableId});  // Use table 0
  task.SetLambda([kTableId, kMaxKey](const Info& info){
    LOG(INFO) << "Hi";
    LOG(INFO) << info.DebugString();
    auto table = info.CreateKVClientTable<float>(kTableId);
    std::vector<Key> keys(kMaxKey);
    std::iota(keys.begin(), keys.end(), 0);
    std::vector<float> vals(keys.size(), 0.5);
    std::vector<float> ret;
    for (int i = 0; i < 100; ++ i) {
      table->Get(keys, &ret);
      table->Add(keys, vals);
      table->Clock();
      CHECK_EQ(ret.size(), keys.size());
      // LOG(INFO) << ret[0];
      LOG(INFO) << "Iter: " << i << " finished on Node " << info.worker_id;
    }
  });

  // 4. Run tasks
  engine.Run(task);

  // 5. Stop engine
  engine.StopEverything();
}

}  // namespace flexps

int main(int argc, char** argv) {
  gflags::ParseCommandLineFlags(&argc, &argv, true);
  google::InitGoogleLogging(argv[0]);
  flexps::Run();
}
