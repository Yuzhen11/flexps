// #include "driver/engine.hpp"

#include "gflags/gflags.h"
#include "glog/logging.h"

#include "driver/node_parser.hpp"
#include "driver/engine.hpp"
#include "worker/kv_client_table.hpp"

#include <algorithm>

DEFINE_int32(my_id, -1, "The process id of this program");
DEFINE_string(config_file, "", "The config file path");

DEFINE_string(kModelType, "", "SSP/SparseSSP");

namespace flexps {

void Run() {
  CHECK_NE(FLAGS_my_id, -1);
  CHECK(!FLAGS_config_file.empty());
  CHECK(FLAGS_kModelType == "SSP" || FLAGS_kModelType == "SparseSSP");
  LOG(INFO) << "Running in " << FLAGS_kModelType << " mode";

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
  const int kStaleness = 0;
  const int kSpeculation = 1;
  std::vector<third_party::Range> range;
  for (int i = 0; i < nodes.size() - 1; ++ i) {
    range.push_back({kMaxKey / nodes.size() * i, kMaxKey / nodes.size() * (i + 1)});
  }
  range.push_back({kMaxKey / nodes.size() * (nodes.size() - 1), kMaxKey});
  if (FLAGS_kModelType == "SSP") {
    engine.CreateTable<float>(kTableId, range, 
        ModelType::SSPModel, StorageType::MapStorage);
  } else if (FLAGS_kModelType == "SparseSSP") {
    engine.CreateTable<float>(kTableId, range, 
        ModelType::SparseSSPModel, StorageType::MapStorage, kStaleness, kSpeculation);
  } else {
    CHECK(false);
  }
  engine.Barrier();

  // 3. Construct tasks
  MLTask task;
  std::vector<WorkerAlloc> worker_alloc;
  for (auto& node : nodes) {
    worker_alloc.push_back({node.id, 1});  // each node has 10 workers
  }
  task.SetWorkerAlloc(worker_alloc);
  task.SetTables({kTableId});  // Use table 0
  task.SetLambda([kTableId, kMaxKey, kSpeculation](const Info& info){
    LOG(INFO) << "Hi";
    LOG(INFO) << info.DebugString();
    const int kNumIters = 100;
    if (FLAGS_kModelType == "SSP") {  // SSP mode
      auto table = info.CreateKVClientTable<float>(kTableId);
      std::vector<Key> keys(kMaxKey);
      std::iota(keys.begin(), keys.end(), 0);
      std::vector<float> vals(keys.size(), 0.5);
      std::vector<float> ret;
      for (int i = 0; i < kNumIters; ++ i) {
        table.Get(keys, &ret);
        table.Add(keys, vals);
        table.Clock();
        CHECK_EQ(ret.size(), keys.size());
        // LOG(INFO) << ret[0];
        LOG(INFO) << "Iter: " << i << " finished on Node " << info.worker_id;
      }
    } else if (FLAGS_kModelType == "SparseSSP") {  // Sparse SSP mode
      // Prepare the future keys
      std::vector<third_party::SArray<Key>> future_keys;
      for (int i = 0; i < kNumIters + kSpeculation; ++ i) {
        CHECK_LT(info.worker_id, kMaxKey);
        future_keys.push_back({info.worker_id});
      }

      auto table = info.CreateSparseKVClientTable<float>(kTableId, kSpeculation, future_keys);
      third_party::SArray<float> rets;
      third_party::SArray<float> vals;
      for (int i = 0; i < kNumIters; ++ i) {
        table.Get(&rets);
        auto& keys = future_keys[i];
        CHECK_EQ(keys.size(), rets.size());
        vals.resize(keys.size());
        for (int i = 0; i < vals.size(); ++ i) vals[i] = 0.5;
        table.Add(keys, vals);
        LOG(INFO) << "Iter: " << i << " finished on Node " << info.worker_id;
      }
    } else {
      CHECK(false);
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
