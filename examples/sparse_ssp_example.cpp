// #include "driver/engine.hpp"

#include "gflags/gflags.h"
#include "glog/logging.h"

#include "driver/engine.hpp"
#include "worker/kv_client_table.hpp"

#include <algorithm>
#include <cstdlib>
#include <chrono>
#include <ctime>

DEFINE_int32(my_id, -1, "The process id of this program");
DEFINE_string(config_file, "", "The config file path");

DEFINE_int32(num_dims, 1000, "number of dimensions");
DEFINE_int32(num_nonzeros, 10, "number of nonzeros");
DEFINE_int32(num_iters, 10, "number of iters");

DEFINE_string(kModelType, "", "ASP/SSP/BSP/SparseSSP");
DEFINE_string(kStorageType, "", "Map/Vector");
DEFINE_int32(kStaleness, 0, "stalness");
DEFINE_int32(kSpeculation, 1, "speculation");
DEFINE_string(kSparseSSPRecorderType, "", "None/Map/Vector");
DEFINE_int32(num_workers_per_node, 1, "num_workers_per_node");
DEFINE_int32(with_injected_straggler, 0, "with injected straggler or not, 0/1");
DEFINE_int32(num_servers_per_node, 1, "num_servers_per_node");

namespace flexps {

std::vector<third_party::SArray<Key>> GenerateRandomKeys(int num_nz, int num_dims, int num_iters) {
  std::vector<third_party::SArray<Key>> keys(num_iters);
  CHECK_LE(num_nz, num_dims);
  CHECK_GT(num_nz, 0);
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
  CHECK(FLAGS_kModelType == "ASP" || FLAGS_kModelType == "BSP" || FLAGS_kModelType == "SSP" || FLAGS_kModelType == "SparseSSP");
  CHECK(FLAGS_kSparseSSPRecorderType == "None" || FLAGS_kSparseSSPRecorderType == "Map" || FLAGS_kSparseSSPRecorderType == "Vector");
  CHECK(FLAGS_kStorageType == "Map" || FLAGS_kStorageType == "Vector");
  CHECK_GT(FLAGS_num_dims, 0);
  CHECK_GT(FLAGS_num_nonzeros, 0);
  CHECK_GT(FLAGS_num_iters, 0);
  CHECK_LE(FLAGS_num_nonzeros, FLAGS_num_dims);
  CHECK_LE(FLAGS_kStaleness, 5);
  CHECK_GE(FLAGS_kStaleness, 0);
  CHECK_LE(FLAGS_kSpeculation, 50);
  CHECK_GE(FLAGS_kSpeculation, 1);
  CHECK_LE(FLAGS_num_workers_per_node, 20);
  CHECK_GE(FLAGS_num_workers_per_node, 1);
  CHECK_LE(FLAGS_num_servers_per_node, 20);
  CHECK_GE(FLAGS_num_servers_per_node, 1);
  CHECK_GE(FLAGS_with_injected_straggler, 0);
  CHECK_LE(FLAGS_with_injected_straggler, 1);

  if (FLAGS_my_id == 0) {
    LOG(INFO) << "Running in " << FLAGS_kModelType << " mode";
    LOG(INFO) << "num_dims: " << FLAGS_num_dims;
    LOG(INFO) << "num_nonzeros: " << FLAGS_num_nonzeros;
    LOG(INFO) << "num_iters: " << FLAGS_num_iters;
    LOG(INFO) << "with_injected_straggler: " << FLAGS_with_injected_straggler;
    LOG(INFO) << "num_workers_per_node: " << FLAGS_num_workers_per_node;
    LOG(INFO) << "num_servers_per_node: " << FLAGS_num_servers_per_node;
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
  engine.StartEverything(FLAGS_num_servers_per_node);

  // 2. Create tables
  const int kTableId = 0;
  std::vector<third_party::Range> range;
  int num_total_servers = nodes.size() * FLAGS_num_servers_per_node;
  for (int i = 0; i < num_total_servers - 1; ++ i) {
    range.push_back({FLAGS_num_dims / num_total_servers * i, FLAGS_num_dims / num_total_servers * (i + 1)});
  }
  range.push_back({FLAGS_num_dims / num_total_servers * (num_total_servers - 1), (uint64_t)FLAGS_num_dims});

  ModelType model_type;
  if (FLAGS_kModelType == "ASP") {
    model_type = ModelType::ASP;
  } else if (FLAGS_kModelType == "SSP") {
    model_type = ModelType::SSP;
  } else if (FLAGS_kModelType == "BSP") {
    model_type = ModelType::BSP;
  } else if (FLAGS_kModelType == "SparseSSP") {
    model_type = ModelType::SparseSSP;
  } else {
    CHECK(false) << "model type error: " << FLAGS_kModelType;
  }

  StorageType storage_type;
  if (FLAGS_kStorageType == "Map") {
    storage_type = StorageType::Map;
  } else if (FLAGS_kStorageType == "Vector") {
    storage_type = StorageType::Vector;
  } else {
    CHECK(false) << "storage type error: " << FLAGS_kStorageType;
  }

  SparseSSPRecorderType sparse_ssp_recorder_type;
  if (FLAGS_kSparseSSPRecorderType == "None") {
    sparse_ssp_recorder_type = SparseSSPRecorderType::None;
  } else if (FLAGS_kSparseSSPRecorderType == "Map") {
    sparse_ssp_recorder_type = SparseSSPRecorderType::Map;
  } else if (FLAGS_kSparseSSPRecorderType == "Vector") {
    sparse_ssp_recorder_type = SparseSSPRecorderType::Vector;
  } else {
    CHECK(false) << "sparse_ssp_storage type error: " << FLAGS_kSparseSSPRecorderType;
  }

  // Create SparseSSP table or normal table
  if (model_type == ModelType::SparseSSP) {
    engine.CreateSparseSSPTable<float>(kTableId, range, 
        model_type, storage_type, FLAGS_kStaleness, FLAGS_kSpeculation, sparse_ssp_recorder_type);
  } else {
    engine.CreateTable<float>(kTableId, range, 
        model_type, storage_type, FLAGS_kStaleness);
  }
  engine.Barrier();

  // 3. Construct tasks
  MLTask task;
  std::vector<WorkerAlloc> worker_alloc;
  for (auto& node : nodes) {
    worker_alloc.push_back({node.id, FLAGS_num_workers_per_node});  // each node has num_workers_per_node workers
  }
  task.SetWorkerAlloc(worker_alloc);
  task.SetTables({kTableId});  // Use table 0
  task.SetLambda([kTableId](const Info& info){
    LOG(INFO) << "Hi";
    LOG(INFO) << info.DebugString();
    // 1. Prepare the future keys
    std::vector<third_party::SArray<Key>> future_keys = GenerateRandomKeys(FLAGS_num_nonzeros, FLAGS_num_dims, FLAGS_num_iters+FLAGS_kSpeculation);
    // std::vector<third_party::SArray<Key>> future_keys = GenerateFixedKeys(FLAGS_num_iters+FLAGS_kSpeculation, info.worker_id);
    // std::vector<third_party::SArray<Key>> future_keys = GenerateAllKeys(FLAGS_num_dims, FLAGS_num_iters+FLAGS_kSpeculation);

    auto start_time = std::chrono::steady_clock::now();
    srand(time(0));

    if (FLAGS_kModelType == "SSP" || FLAGS_kModelType == "ASP" || FLAGS_kModelType == "BSP") {  // normal mode
      auto table = info.CreateKVClientTable<float>(kTableId);
      third_party::SArray<float> rets;
      third_party::SArray<float> vals;
      for (int i = 0; i < FLAGS_num_iters; ++ i) {
        CHECK_LT(i, future_keys.size());
        auto& keys = future_keys[i];
        table->Get(keys, &rets);
        CHECK_EQ(keys.size(), rets.size());
        vals.resize(keys.size());
        for (int i = 0; i < vals.size(); ++ i) vals[i] = 0.5;
        table->Add(keys, vals);
        table->Clock();
        CHECK_EQ(rets.size(), keys.size());
        if (i % 10 == 0)
          LOG(INFO) << "Iter: " << i << " finished on worker " << info.worker_id;

        if (FLAGS_with_injected_straggler) {
          double r = (double)rand() / RAND_MAX;
          if (r < 0.05) {
            double delay = (double)rand() / RAND_MAX * 100;
            std::this_thread::sleep_for(std::chrono::milliseconds(int(delay)));
            LOG(INFO) << "sleep for " << int(delay) << " ms";
          }
        }
      }
    } else if (FLAGS_kModelType == "SparseSSP") {  // Sparse SSP mode
      auto table = info.CreateSparseKVClientTable<float>(kTableId, FLAGS_kSpeculation, future_keys);
      third_party::SArray<float> rets;
      third_party::SArray<float> vals;
      for (int i = 0; i < FLAGS_num_iters; ++ i) {
        CHECK_LT(i, future_keys.size());
        auto& keys = future_keys[i];
        table->Get(&rets);
        CHECK_EQ(keys.size(), rets.size());
        vals.resize(keys.size());
        for (int i = 0; i < vals.size(); ++ i) vals[i] = 0.5;
        table->Add(keys, vals);
        if (i % 10 == 0)
          LOG(INFO) << "Iter: " << i << " finished on worker " << info.worker_id;

        if (FLAGS_with_injected_straggler) {
          double r = (double)rand() / RAND_MAX;
          if (r < 0.05) {
            double delay = (double)rand() / RAND_MAX * 100;
            std::this_thread::sleep_for(std::chrono::milliseconds(int(delay)));
            LOG(INFO) << "sleep for " << int(delay) << " ms";
          }
        }
      }

    } else {
      CHECK(false);
    }

    auto end_time = std::chrono::steady_clock::now();
    auto total_time = std::chrono::duration_cast<std::chrono::milliseconds>(end_time - start_time).count();
    LOG(INFO) << "total time: " << total_time << " ms on worker: " << info.worker_id;
  });

  // if (my_node.id == 0) {
  //   ProfilerStart("/data/opt/tmp/a.prof");
  // }

  // 4. Run tasks
  engine.Run(task);

  // if (my_node.id == 0) {
  //   ProfilerStop();
  // }

  // 5. Stop engine
  engine.StopEverything();
}

}  // namespace flexps

int main(int argc, char** argv) {
  gflags::ParseCommandLineFlags(&argc, &argv, true);
  google::InitGoogleLogging(argv[0]);
  flexps::Run();
}
