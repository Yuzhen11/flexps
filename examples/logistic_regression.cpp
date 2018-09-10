#include "gflags/gflags.h"
#include "glog/logging.h"

#include <algorithm>
#include <chrono>
#include <cstdlib>
#include <ctime>
#include <thread>
#include <cmath>

#include "boost/utility/string_ref.hpp"
#include "base/serialization.hpp"
#include "io/hdfs_manager.hpp"
#include "lib/batch_data_sampler.cpp"
#include "lib/libsvm_parser.cpp"
#include "driver/engine.hpp"
#include "worker/kv_client_table.hpp"

DEFINE_int32(my_id, -1, "The process id of this program");
DEFINE_string(config_file, "", "The config file path");
DEFINE_string(hdfs_namenode, "", "The hdfs namenode hostname");
DEFINE_string(input, "", "The hdfs input url");
DEFINE_int32(hdfs_namenode_port, -1, "The hdfs namenode port");

DEFINE_string(kModelType, "", "ASP/SSP/BSP/SparseSSP");
DEFINE_string(kStorageType, "", "Map/Vector");
DEFINE_int32(num_dims, 0, "number of dimensions");
DEFINE_int32(batch_size, 100, "batch size of each epoch");
DEFINE_int32(num_iters, 10, "number of iters");
DEFINE_int32(kStaleness, 0, "stalness");
DEFINE_int32(kSpeculation, 1, "speculation");
DEFINE_string(kSparseSSPRecorderType, "", "None/Map/Vector");
DEFINE_int32(num_workers_per_node, 1, "num_workers_per_node");
DEFINE_int32(with_injected_straggler, 0, "with injected straggler or not, 0/1");
DEFINE_int32(num_servers_per_node, 1, "num_servers_per_node");
DEFINE_double(alpha, 0.1, "learning rate");

namespace flexps {

template<typename T>
void test_error(third_party::SArray<float>& rets_w, std::vector<T>& data_) {

  int count = 0;
  float c_count = 0;  /// correct count
  for (int i = 0; i < data_.size(); ++i) {
    auto& data = data_[i];
    count = count + 1;
    auto& x = data.first;
    float y = data.second;
    if (y < 0)
      y = 0;

    float pred_y = 0.0;
    for (auto field : x)
      pred_y += rets_w[field.first] * field.second;

    pred_y = 1. / (1. + exp(-1 * pred_y));
    pred_y = (pred_y > 0.5) ? 1 : 0;
    if (int(pred_y) == int(y)) {
        c_count += 1;
    }
  }
  LOG(INFO) << " accuracy is " << std::to_string(c_count / count);
}

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

  // 1. Load data
  HDFSManager::Config config;
  config.input = FLAGS_input;
  config.worker_host = my_node.hostname;
  config.worker_port = my_node.port;
  config.master_port = 19715;
  config.master_host = nodes[0].hostname;
  config.hdfs_namenode = FLAGS_hdfs_namenode;
  config.hdfs_namenode_port = FLAGS_hdfs_namenode_port;
  config.num_local_load_thread = FLAGS_num_workers_per_node;

  // DataObj = <feature<key, val>, label>
  using DataObj = std::pair<std::vector<std::pair<int, float>>, float>;

  zmq::context_t* zmq_context = new zmq::context_t(1);
  HDFSManager hdfs_manager(my_node, nodes, config, zmq_context);
  LOG(INFO) << "manager set up";
  hdfs_manager.Start();
  LOG(INFO) << "manager start";

  std::vector<DataObj> data;
  std::mutex mylock;
  hdfs_manager.Run([my_node, &data, &mylock](HDFSManager::InputFormat* input_format, int local_tid) {
    
    DataObj this_obj;
    while (input_format->HasRecord()) {
      auto record = input_format->GetNextRecord();
      if (record.empty()) return;
      this_obj = libsvm_parser(record);

      mylock.lock();    
      data.push_back(std::move(this_obj));
      mylock.unlock();
    }
    LOG(INFO) << data.size() << " lines in (node, thread):(" 
      << my_node.id << "," << local_tid << ")";
  });
  hdfs_manager.Stop();
  LOG(INFO) << "Finished loading data!";
  int count = 0;
  for (int i = 0; i < 10; i++) {
  count += data[i].first.size();
  }
  LOG(INFO) << "Estimated number of non-zero: " << count / 10;

  // 2. Start engine
  Engine engine(my_node, nodes);
  engine.StartEverything();

  // 3. Create tables
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
  task.SetLambda([kTableId, &data](const Info& info){
    LOG(INFO) << info.DebugString();

    BatchDataSampler<DataObj> batch_data_sampler(data, FLAGS_batch_size);
    //prepare all_keys
    third_party::SArray<Key> all_keys;
    for (int i = 0; i < FLAGS_num_dims; ++ i)
      all_keys.push_back(i);

    // prepare future_keys
    std::vector<third_party::SArray<Key>> future_keys;
    std::vector<std::vector<DataObj*>> future_data_ptrs;
    
    for (int i = 0; i < FLAGS_num_iters + FLAGS_kSpeculation; ++ i) {
      batch_data_sampler.random_start_point();
      future_keys.push_back(batch_data_sampler.prepare_next_batch());  // prepare keys
      future_data_ptrs.push_back(batch_data_sampler.get_data_ptrs()); // prepare ptrs to data
    }
    CHECK_EQ(future_keys.size(), FLAGS_num_iters + FLAGS_kSpeculation);

    auto start_time = std::chrono::steady_clock::now();
    std::chrono::steady_clock::time_point end_time;
    srand(time(0));
    //　TO DO: make it real LR algorithm
    if (FLAGS_kModelType == "SSP" || FLAGS_kModelType == "ASP" || FLAGS_kModelType == "BSP") {  // normal mode
      auto table = info.CreateKVClientTable<float>(kTableId);
      third_party::SArray<float> params;
      third_party::SArray<float> deltas;
      for (int i = 0; i < FLAGS_num_iters; ++ i) {
        CHECK_LT(i, future_keys.size());
        auto& keys = future_keys[i];
        table->Get(keys, &params);
        CHECK_EQ(keys.size(), params.size());
        deltas.resize(keys.size(), 0.0);

        for (auto data : future_data_ptrs[i]) {  // iterate over the data in the batch
            auto& x = data->first;
            float y = data->second;
            if (y < 0)
                y = 0;
            float pred_y = 0.0;
            int j = 0;
            for (auto field : x) {
                while (keys[j] < field.first)
                    j += 1;
                pred_y += params[j] * field.second;
            }
            pred_y = 1. / (1. + exp(-1 * pred_y));
            j = 0;
            for (auto field : x) {
                while (keys[j] < field.first)
                    j += 1;
                deltas[j] += FLAGS_alpha * field.second * (y - pred_y);
            }
        }
        table->Add(keys, deltas);  // issue Push
        table->Clock();
        CHECK_EQ(params.size(), keys.size());

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
      end_time = std::chrono::steady_clock::now();

      // test error
      table->Get(all_keys, &params);
      test_error<DataObj>(params, data);
    } 
    else if (FLAGS_kModelType == "SparseSSP") {  // Sparse SSP mode

      auto table = info.CreateSparseKVClientTable<float>(kTableId, FLAGS_kSpeculation, future_keys);
      third_party::SArray<float> params;
      third_party::SArray<float> deltas;
      for (int i = 0; i < FLAGS_num_iters; ++ i) {
        CHECK_LT(i, future_keys.size());
        auto& keys = future_keys[i];
        table->Get(&params);
        CHECK_EQ(keys.size(), params.size());
        deltas.resize(keys.size(), 0.0);
        
        for (auto data : future_data_ptrs[i]) {  // iterate over the data in the batch
            auto& x = data->first;
            float y = data->second;
            if (y < 0)
                y = 0;
            float pred_y = 0.0;
            int j = 0;
            for (auto field : x) {
                while (keys[j] < field.first)
                    j += 1;
                pred_y += params[j] * field.second;
            }
            pred_y = 1. / (1. + exp(-1 * pred_y));
            j = 0;
            for (auto field : x) {
                while (keys[j] < field.first)
                    j += 1;
                deltas[j] += FLAGS_alpha * field.second * (y - pred_y);
            }
        }

        table->Add(keys, deltas);
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
      end_time = std::chrono::steady_clock::now();
    }
    else {
      CHECK(false);
    }
    auto total_time = std::chrono::duration_cast<std::chrono::milliseconds>(end_time - start_time).count();
    LOG(INFO) << "total time: " << total_time << " ms on worker: " << info.worker_id;
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
