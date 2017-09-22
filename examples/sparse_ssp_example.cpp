// #include "driver/engine.hpp"

#include "gflags/gflags.h"
#include "glog/logging.h"

#include "driver/node_parser.hpp"
#include "driver/engine.hpp"
#include "worker/kv_client_table.hpp"

#include <algorithm>
#include <cstdlib>

DEFINE_int32(my_id, -1, "The process id of this program");
DEFINE_string(config_file, "", "The config file path");

DEFINE_string(kModelType, "", "SSP/SparseSSP");

DEFINE_int32(num_dims, 1000, "number of dimensions");
DEFINE_int32(num_nonzeros, 10, "number of nonzeros");
DEFINE_int32(num_iters, 10, "number of iters");

namespace flexps {

std::vector<third_party::SArray<Key>> GenerateRandomKeys(int num_nz, int num_dims, int num_iters) {
  std::vector<third_party::SArray<Key>> keys(num_iters);
  CHECK_LE(num_nz, num_dims);
  CHECK_GT(num_nz, 0);
  // The num_nz is estimated.
  for (int i = 0; i < num_iters; ++ i) {
    std::set<Key> s;
    int j = 0;
    while (j < num_nz) {
      Key k = rand() % num_dims;
      if (s.find(k) == s.end()) {
        s.insert(k);
        j++;
      }
    }
    for (auto k : s) {
      keys[i].push_back(k);
    }
    CHECK_EQ(keys[i].size(), num_nz);
  }
  return keys;
}

std::vector<third_party::SArray<Key>> GenerateFixedKeys(int num_iters, int worker_id) {
  std::vector<third_party::SArray<Key>> keys(num_iters);
  for (int i = 0; i < keys.size(); ++ i) {
    keys[i] = {worker_id};
  }
  return keys;
}

std::vector<third_party::SArray<Key>> GenerateAllKeys(int num_dims, int num_iters) {
  std::vector<third_party::SArray<Key>> keys(num_iters);
  for (int i = 0; i < keys.size(); ++ i) {
    for (int j = 0; j < num_dims; ++ j) {
      keys[i].push_back(j);
    }
  }
  return keys;
}

void Run() {
  srand(0);
  CHECK_NE(FLAGS_my_id, -1);
  CHECK(!FLAGS_config_file.empty());
  CHECK(FLAGS_kModelType == "SSP" || FLAGS_kModelType == "SparseSSP");
  CHECK_GT(FLAGS_num_dims, 0);
  CHECK_GT(FLAGS_num_nonzeros, 0);
  CHECK_GT(FLAGS_num_iters, 0);
  CHECK_LE(FLAGS_num_nonzeros, FLAGS_num_dims);

  if (FLAGS_my_id == 0) {
    LOG(INFO) << "Running in " << FLAGS_kModelType << " mode";
    LOG(INFO) << "num_dims: " << FLAGS_num_dims;
    LOG(INFO) << "num_nonzeros: " << FLAGS_num_nonzeros;
    LOG(INFO) << "num_iters: " << FLAGS_num_iters;
  }

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
  const int kStaleness = 0;
  const int kSpeculation = 1;
  std::vector<third_party::Range> range;
  for (int i = 0; i < nodes.size() - 1; ++ i) {
    range.push_back({FLAGS_num_dims / nodes.size() * i, FLAGS_num_dims / nodes.size() * (i + 1)});
  }
  range.push_back({FLAGS_num_dims / nodes.size() * (nodes.size() - 1), (uint64_t)FLAGS_num_dims});
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
    worker_alloc.push_back({node.id, 3});  // each node has 10 workers
  }
  task.SetWorkerAlloc(worker_alloc);
  task.SetTables({kTableId});  // Use table 0
  task.SetLambda([kTableId, kSpeculation](const Info& info){
    LOG(INFO) << "Hi";
    LOG(INFO) << info.DebugString();
    // 1. Prepare the future keys
    // std::vector<third_party::SArray<Key>> future_keys = GenerateRandomKeys(FLAGS_num_nonzeros, FLAGS_num_dims, FLAGS_num_iters+kSpeculation);
    // std::vector<third_party::SArray<Key>> future_keys = GenerateFixedKeys(FLAGS_num_iters+kSpeculation, info.worker_id);
    std::vector<third_party::SArray<Key>> future_keys = GenerateAllKeys(FLAGS_num_dims, FLAGS_num_iters+kSpeculation);

    if (FLAGS_kModelType == "SSP") {  // SSP mode
      auto table = info.CreateKVClientTable<float>(kTableId);
      third_party::SArray<float> rets;
      third_party::SArray<float> vals;
      for (int i = 0; i < FLAGS_num_iters; ++ i) {
        CHECK_LT(i, future_keys.size());
        auto& keys = future_keys[i];
        table.Get(keys, &rets);
        CHECK_EQ(keys.size(), rets.size());
        vals.resize(keys.size());
        for (int i = 0; i < vals.size(); ++ i) vals[i] = 0.5;
        table.Add(keys, vals);
        table.Clock();
        CHECK_EQ(rets.size(), keys.size());
        LOG(INFO) << "Iter: " << i << " finished on worker " << info.worker_id;
      }
    } else if (FLAGS_kModelType == "SparseSSP") {  // Sparse SSP mode
      auto table = info.CreateSparseKVClientTable<float>(kTableId, kSpeculation, future_keys);
      third_party::SArray<float> rets;
      third_party::SArray<float> vals;
      for (int i = 0; i < FLAGS_num_iters; ++ i) {
        CHECK_LT(i, future_keys.size());
        auto& keys = future_keys[i];
        table.Get(&rets);
        CHECK_EQ(keys.size(), rets.size());
        vals.resize(keys.size());
        for (int i = 0; i < vals.size(); ++ i) vals[i] = 0.5;
        table.Add(keys, vals);
        LOG(INFO) << "Iter: " << i << " finished on worker " << info.worker_id;
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
