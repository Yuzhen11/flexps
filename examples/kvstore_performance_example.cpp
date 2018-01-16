#include "gflags/gflags.h"
#include "glog/logging.h"

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
  const int kMaxKey = 100000000;
  const int kStaleness = 3;
  std::vector<third_party::Range> range;
  range.push_back({0, kMaxKey / 2});
  range.push_back({kMaxKey / 2, kMaxKey});

  engine.CreateTable<float>(kTableId, range, 
      ModelType::SSP, StorageType::Vector, kStaleness);
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

      // using Sarray to store the keys and vals
      LOG(INFO) << "Using Sarray!\n";
      third_party::SArray<Key> sarray_keys1(kMaxKey / 2);
      third_party::SArray<Key> sarray_keys2(kMaxKey / 2);

      std::iota(sarray_keys1.begin(), sarray_keys1.end(), 0);
      std::iota(sarray_keys2.begin(), sarray_keys2.end(), kMaxKey / 2);

      third_party::SArray<float> sarray_vals(kMaxKey / 2, 0.5);
      third_party::SArray<float> sarray_rets;

      // Worker (0) and server (0) are in the same node
      auto start_time = std::chrono::steady_clock::now();
      table->Add(sarray_keys1, sarray_vals);

      auto start_time_2 = std::chrono::steady_clock::now();
      table->Get(sarray_keys1, &sarray_rets);
      table->Clock();

      auto end_time = std::chrono::steady_clock::now();
      auto total_time = std::chrono::duration_cast<std::chrono::milliseconds>(end_time - start_time).count();
      auto total_time_2 = std::chrono::duration_cast<std::chrono::milliseconds>(end_time - start_time_2).count();
      
      LOG(INFO) << "Total time of Get from server in the same node: " << total_time_2 << " ms";
      LOG(INFO) << "Total time of Add to and Get from server in the same node: " << total_time << " ms";

      DCHECK_EQ(sarray_rets.size(), sarray_keys1.size());


      // Worker (0) and server (1) are in different nodes
      start_time = std::chrono::steady_clock::now();
      table->Add(sarray_keys2, sarray_vals);

      start_time_2 = std::chrono::steady_clock::now();
      table->Get(sarray_keys2, &sarray_rets);
      table->Clock();

      end_time = std::chrono::steady_clock::now();
      total_time = std::chrono::duration_cast<std::chrono::milliseconds>(end_time - start_time).count();
      total_time_2 = std::chrono::duration_cast<std::chrono::milliseconds>(end_time - start_time_2).count();
      
      LOG(INFO) << "Total time of Get from server in different node: " << total_time_2 << " ms";
      LOG(INFO) << "Total time of Add to and Get from server in different node: " << total_time << " ms";

      DCHECK_EQ(sarray_rets.size(), sarray_keys2.size());



      // using vector to store the keys and vals
      LOG(INFO) << "Using vector!\n";
      std::vector<Key> vector_keys1(kMaxKey / 2);
      std::vector<Key> vector_keys2(kMaxKey / 2);

      std::iota(vector_keys1.begin(), vector_keys1.end(), 0);
      std::iota(vector_keys2.begin(), vector_keys2.end(), kMaxKey / 2);

      std::vector<float> vector_vals(kMaxKey / 2, 0.5);
      std::vector<float> vector_rets;

      // Worker (0) and server (0) are in the same node
      start_time = std::chrono::steady_clock::now();
      table->Add(vector_keys1, vector_vals);

      start_time_2 = std::chrono::steady_clock::now();
      table->Get(vector_keys1, &vector_rets);
      table->Clock();

      end_time = std::chrono::steady_clock::now();
      total_time = std::chrono::duration_cast<std::chrono::milliseconds>(end_time - start_time).count();
      total_time_2 = std::chrono::duration_cast<std::chrono::milliseconds>(end_time - start_time_2).count();
      
      LOG(INFO) << "Total time of Get from server in the same node: " << total_time_2 << " ms";
      LOG(INFO) << "Total time of Add to and Get from server in the same node: " << total_time << " ms";

      DCHECK_EQ(vector_rets.size(), vector_keys1.size());


      // Worker (0) and server (1) are in different nodes
      start_time = std::chrono::steady_clock::now();
      table->Add(vector_keys2, vector_vals);

      start_time_2 = std::chrono::steady_clock::now();
      table->Get(vector_keys2, &vector_rets);
      table->Clock();

      end_time = std::chrono::steady_clock::now();
      total_time = std::chrono::duration_cast<std::chrono::milliseconds>(end_time - start_time).count();
      total_time_2 = std::chrono::duration_cast<std::chrono::milliseconds>(end_time - start_time_2).count();
      
      LOG(INFO) << "Total time of Get from server in different node: " << total_time_2 << " ms";
      LOG(INFO) << "Total time of Add to and Get from server in different node: " << total_time << " ms";

      DCHECK_EQ(vector_rets.size(), vector_keys2.size());
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
