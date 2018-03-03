#include "gflags/gflags.h"
#include "glog/logging.h"

#include "boost/utility/string_ref.hpp"
#include "base/node_util.hpp"
#include "base/serialization.hpp"
#include "comm/channel.hpp"
#include "comm/mailbox.hpp"
#include "driver/engine.hpp"
#include "driver/simple_id_mapper.hpp"

#include "io/hdfs_manager.hpp"
#include "lib/batch_data_sampler.cpp"
#include "lib/libsvm_parser.cpp"
#include "worker/kv_client_table.hpp"

#include <map>
#include <string>
#include <cstdlib>
// GBDT src
#include "examples/gbdt/src/channel_util.hpp"
#include "examples/gbdt/src/data_loader.hpp"
#include "examples/gbdt/src/gbdt_controller.hpp"
#include "examples/gbdt/src/math_tools.hpp"
#include "examples/gbdt/src/util.hpp"

DEFINE_int32(my_id, -1, "The process id of this program");
DEFINE_string(config_file, "", "The config file path");
DEFINE_int32(num_workers_per_node, 1, "The number of workers per node");
DEFINE_string(run_mode, "", "The running mode of application");
DEFINE_int32(kStaleness, 0, "stalness");

// hdfs mode config
DEFINE_string(hdfs_namenode, "", "The hdfs namenode hostname");
DEFINE_int32(hdfs_namenode_port, -1, "The hdfs namenode port");
DEFINE_string(cluster_train_input, "", "The hdfs train input url");
DEFINE_string(cluster_test_input, "", "The hdfs test input url");

// Local mode config
DEFINE_string(local_train_input, "", "The local train input path");
DEFINE_string(local_test_input, "", "The local test input path");

// gbdt model config
DEFINE_int32(num_of_trees, 1, "The number of trees in forest");
DEFINE_int32(max_depth, 0, "The max depth of each tree");
DEFINE_double(complexity_of_leaf, 0.0, "The complexity of leaf");
DEFINE_double(rank_fraction, 0.1, "The rank fraction of quantile sketch");

namespace flexps {

std::vector<std::string> load_data_from_hdfs(Node & my_node, std::vector<Node> & nodes, std::string input_path) {
  HDFSManager::Config config;
  config.input = input_path;
  config.worker_host = my_node.hostname;
  config.worker_port = my_node.port;
  config.master_port = 19715;
  config.master_host = nodes[0].hostname;
  config.hdfs_namenode = FLAGS_hdfs_namenode;
  config.hdfs_namenode_port = FLAGS_hdfs_namenode_port;
  config.num_local_load_thread = FLAGS_num_workers_per_node;
  
  zmq::context_t* zmq_context = new zmq::context_t(1);
  HDFSManager hdfs_manager(my_node, nodes, config, zmq_context);
  LOG(INFO) << "manager set up";
  hdfs_manager.Start();
  LOG(INFO) << "manager start";

  std::vector<std::string> data;
  std::mutex mylock;
  hdfs_manager.Run([my_node, &data, &mylock](HDFSManager::InputFormat* input_format, int local_tid) {
    
    while (input_format->HasRecord()) {
      auto record = input_format->GetNextRecord();
      if (record.empty()) return;
      mylock.lock();    
      data.push_back(std::string(record.begin(), record.end()));
      mylock.unlock();

    }
    LOG(INFO) << data.size() << " lines in (node, thread):(" 
      << my_node.id << "," << local_tid << ")";
  });
  hdfs_manager.Stop();
  LOG(INFO) << "Finished loading data!";
  return data;
}

void distribute_data_to_worker(int data_num, int worker_id, int worker_num, std::vector<float> & all_class_vect, std::vector<float> & class_vect
  , std::vector<std::vector<float>> & all_feat_vect_list, std::vector<std::vector<float>> & feat_vect_list) {
  // Distribute data to each worker
  int num_of_records_per_worker = data_num / worker_num;

  if (worker_id == worker_num - 1) {
    class_vect.insert(class_vect.end(), all_class_vect.begin() + (worker_id * num_of_records_per_worker), all_class_vect.end());
    for (int i = 0; i < feat_vect_list.size(); i++) {
      feat_vect_list[i].insert(feat_vect_list[i].end(), all_feat_vect_list[i].begin() + (worker_id * num_of_records_per_worker), all_feat_vect_list[i].end());
    }
  }
  else {
    class_vect.insert(class_vect.end(), all_class_vect.begin() + (worker_id * num_of_records_per_worker), 
      all_class_vect.begin() + ((worker_id + 1) * num_of_records_per_worker));
    for (int i = 0; i < feat_vect_list.size(); i++) {
      feat_vect_list[i].insert(feat_vect_list[i].end(), all_feat_vect_list[i].begin() + (worker_id * num_of_records_per_worker),
        all_feat_vect_list[i].begin() + ((worker_id + 1) * num_of_records_per_worker));
    }
  }
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

  // Load data (HDFS)
  LOG(INFO) << "Load training dataset";
  std::vector<std::string> train_data = load_data_from_hdfs(my_node, nodes, FLAGS_cluster_train_input);
  
  std::vector<float> all_class_vect;
  std::vector<std::vector<float>> all_feat_vect_list;
  DataLoader train_data_loader;
  train_data_loader.read_hdfs_to_class_feat_vect(train_data);

  int global_data_num = 0;
  channel_for_balancing_hdfs_data(train_data_loader, global_data_num, my_node, nodes);
  all_feat_vect_list = train_data_loader.get_feat_vect_list();
  
  // Find min and max for each feature
  std::vector<std::map<std::string, float>> min_max_feat_list;
  for (int f_id = 0; f_id < all_feat_vect_list.size(); f_id++) {
    min_max_feat_list.push_back(find_min_max(all_feat_vect_list[f_id]));
  }
  
  std::vector<std::map<std::string, float>> global_min_max_feat_list = channel_for_global_min_max_feat(min_max_feat_list, my_node, nodes);

  // Load testing set
  LOG(INFO) << "Load test dataset";
  std::vector<std::string> test_data = load_data_from_hdfs(my_node, nodes, FLAGS_cluster_test_input);
  DataLoader test_data_loader;
  test_data_loader.read_hdfs_to_class_feat_vect(test_data);

  // 1. Start engine
  const int nodeId = my_node.id;
  Engine engine(my_node, nodes);
  engine.StartEverything();

  // 2. Create tables
  // Create 3 tables: quantile sketch, grad and hess, grad sum and count
  double num_of_feat = global_min_max_feat_list.size();
  double size_on_kv_table = (1 / (0.1 * FLAGS_rank_fraction)) * num_of_feat     // quantile sketch
                          + ((1 / FLAGS_rank_fraction) - 1) * 4 * num_of_feat   // grad and hess
                          + 2                                                   // gard sum and count
                          ;

  const int kMaxKey = (int) (size_on_kv_table + 0.5);   // add 0.5 to do proper rounding
  LOG(INFO) << "Sized required on KV table: " << kMaxKey;
  const int kStaleness = FLAGS_kStaleness;
  
  std::map<std::string, int> kv_table_name_to_tid = {
    {"quantile_sketch", 0},
    {"grad_and_hess", 1},
    {"grad_sum_and_count", 2}
  };

  std::vector<third_party::Range> range;
  for (std::map<std::string, int>::iterator it = kv_table_name_to_tid.begin(); it != kv_table_name_to_tid.end(); it++) {
    int tid = it->second;

    range.clear();
    for (int i = 0; i < nodes.size() - 1; ++ i) {
      range.push_back({kMaxKey / nodes.size() * i, kMaxKey / nodes.size() * (i + 1)});
    }
    range.push_back({kMaxKey / nodes.size() * (nodes.size() - 1), kMaxKey});

    engine.CreateTable<float>(tid, range, ModelType::SSP, StorageType::Map, kStaleness);
  }

  engine.Barrier();
  
  // 3. Construct tasks
  LOG(INFO) << "3. construct task";
  int worker_num = (int) FLAGS_num_workers_per_node;
  MLTask task;
  std::vector<WorkerAlloc> worker_alloc;
  for (auto& node : nodes) {
    worker_alloc.push_back({node.id, worker_num});  // each node has worker_num workers
  }
  task.SetWorkerAlloc(worker_alloc);
  task.SetTables({0, 1, 2});
  
  std::map<std::string, float> all_predict_result;
  all_predict_result["sse"] = 0;
  all_predict_result["num"] = 0;
  task.SetLambda([nodeId, worker_num, & all_predict_result
      , & kv_table_name_to_tid, & train_data_loader, & test_data_loader, & global_min_max_feat_list, & global_data_num](const Info& info) {
    std::vector<std::vector<float>> all_feat_vect_list = train_data_loader.get_feat_vect_list();
    std::vector<float> all_class_vect = train_data_loader.get_class_vect();
    int data_num = all_class_vect.size();

    std::vector<std::vector<float>> feat_vect_list(all_feat_vect_list.size());
    std::vector<float> class_vect;
    
    // Distribute data to each worker (training data)
    distribute_data_to_worker(data_num, info.local_id, worker_num
      , all_class_vect, class_vect, all_feat_vect_list, feat_vect_list);
    
    // Step 0: Initialize setting
    std::map<std::string, std::unique_ptr<KVClientTable<float>>> name_to_kv_table;
    for (std::map<std::string, int>::iterator it = kv_table_name_to_tid.begin(); it != kv_table_name_to_tid.end(); it++) {
      std::string table_name = it->first;
      int tid = it->second;

      auto table = info.CreateKVClientTable<float>(tid);
      name_to_kv_table[table_name] = std::move(table);
    }

    std::map<std::string, float> params = {
      {"num_of_trees", FLAGS_num_of_trees},
      {"max_depth", FLAGS_max_depth},
      {"complexity_of_leaf", FLAGS_complexity_of_leaf},
      {"rank_fraction", FLAGS_rank_fraction},
      {"total_data_num", (float) global_data_num},
      // worker info
      {"node_id", nodeId},
      {"worker_id", info.worker_id}
    };

    std::map<std::string, std::string> options = {
      {"loss_function", "square_error"}
    };

    GBDTController controller;

    // Step 1: Load dataset
    controller.init(options, params);
    controller.build_forest(name_to_kv_table, class_vect, feat_vect_list, global_min_max_feat_list);

    // Show prediction result for test dataset
    all_feat_vect_list = test_data_loader.get_feat_vect_list();
    all_class_vect = test_data_loader.get_class_vect();
    data_num = all_class_vect.size();

    feat_vect_list.clear();
    class_vect.clear();
    feat_vect_list.resize(all_feat_vect_list.size());

    // Distribute data to each worker (test data)
    distribute_data_to_worker(data_num, info.local_id, worker_num
      , all_class_vect, class_vect, all_feat_vect_list, feat_vect_list);
    
    
    std::map<std::string, float> predict_result;
    controller.calculate_predict_result(feat_vect_list, class_vect, predict_result);

    // aggregate all result from workers in this node
    all_predict_result["sse"] += predict_result["sse"];
    all_predict_result["num"] += predict_result["num"];
  });

  // 4. Run tasks
  engine.Run(task);

  // 5. Stop engine
  engine.StopEverything();

  // Use channel to aggregate global SSE and calculate RMSE
  channel_for_global_predict_result(all_predict_result, my_node, nodes);
}

}

int main(int argc, char** argv) {
  gflags::ParseCommandLineFlags(&argc, &argv, true);
  google::InitGoogleLogging(argv[0]);
  LOG(INFO) << "argv[0]: " << argv[0];
  flexps::Run();
}
