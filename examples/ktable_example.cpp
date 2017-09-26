#include "gflags/gflags.h"
#include "glog/logging.h"

#include "driver/node_parser.hpp"
#include "driver/engine.hpp"
#include "worker/kv_client_table.hpp"

#include <algorithm>

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
  const int kMaxKey = 1000000;
  const int kStaleness = 1;
  std::vector<third_party::Range> range;
  range.push_back({0, kMaxKey / 2});
  range.push_back({kMaxKey / 2, kMaxKey});

  engine.CreateTable<float>(kTableId, range, 
      ModelType::SSPModel, StorageType::VectorStorage, kStaleness);
  engine.Barrier();

  // 3. Construct tasks
  MLTask task;
  std::vector<WorkerAlloc> worker_alloc;
  for (auto& node : nodes) {
    worker_alloc.push_back({node.id, 1});  // each node has 1 workers
  }
  task.SetWorkerAlloc(worker_alloc);
  task.SetTables({kTableId});  // Use table 0
  task.SetLambda([kTableId, kMaxKey](const Info& info){

  if (info.worker_id == 0){
    LOG(INFO) << info.DebugString();
    auto table = info.CreateKVClientTable<float>(kTableId);

    third_party::SArray<Key> keys1(kMaxKey / 2);
    third_party::SArray<Key> keys2(kMaxKey / 2);

    std::iota(keys1.begin(), keys1.end(), 0);
    std::iota(keys2.begin(), keys2.end(), kMaxKey / 2);

    third_party::SArray<float> vals(kMaxKey / 2, 0.5);
    third_party::SArray<float> ret;

    // Worker (0) and server (0) are in the same node
    auto start_time = std::chrono::steady_clock::now();
    table.Get(keys1, &ret);
    table.Add(keys1, vals);
    table.Clock();
    auto end_time = std::chrono::steady_clock::now();
    auto total_time = std::chrono::duration_cast<std::chrono::milliseconds>(end_time - start_time).count();
    LOG(INFO) << "Total time of Add and Get to server in the same node: " << total_time << " ms";
    DCHECK_EQ(ret.size(), keys1.size());

    // Worker (0) and server (1) are in different nodes
    start_time = std::chrono::steady_clock::now();
    table.Get(keys2, &ret);
    table.Add(keys2, vals);
    table.Clock();
    end_time = std::chrono::steady_clock::now();
    total_time = std::chrono::duration_cast<std::chrono::milliseconds>(end_time - start_time).count();
    LOG(INFO) << "Total time of Add and Get to server in the different node: " << total_time << " ms";
    DCHECK_EQ(ret.size(), keys2.size());
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
