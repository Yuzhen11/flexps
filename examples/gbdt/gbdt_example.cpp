#include "gflags/gflags.h"
#include "glog/logging.h"

#include "boost/utility/string_ref.hpp"
#include "base/serialization.hpp"
#include "io/hdfs_manager.hpp"
#include "lib/batch_data_sampler.cpp"
#include "lib/libsvm_parser.cpp"
#include "driver/engine.hpp"
#include "worker/kv_client_table.hpp"


#include <map>
#include <string>
// GBDT src
#include "examples/gbdt/src/data_loader.hpp"
#include "examples/gbdt/src/gbdt_controller.hpp"
#include "examples/gbdt/src/math_tools.hpp"

DEFINE_int32(my_id, -1, "The process id of this program");
DEFINE_string(config_file, "", "The config file path");
DEFINE_string(hdfs_namenode, "", "The hdfs namenode hostname");
DEFINE_string(input, "", "The hdfs input url");
DEFINE_int32(hdfs_namenode_port, -1, "The hdfs namenode port");

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

  // Load data
  HDFSManager::Config config;
  config.input = FLAGS_input;
  config.worker_host = my_node.hostname;
  config.worker_port = my_node.port;
  config.master_port = 19715;
  config.master_host = nodes[0].hostname;
  config.hdfs_namenode = FLAGS_hdfs_namenode;
  config.hdfs_namenode_port = FLAGS_hdfs_namenode_port;
  //config.num_local_load_thread = FLAGS_num_workers_per_node;
  config.num_local_load_thread = 3;

  //using DataObj = std::string;

  zmq::context_t* zmq_context = new zmq::context_t(1);
  HDFSManager hdfs_manager(my_node, nodes, config, zmq_context);
  LOG(INFO) << "manager set up";
  hdfs_manager.Start();
  LOG(INFO) << "manager start";

  std::vector<std::string> data;
  std::mutex mylock;
  hdfs_manager.Run([my_node, &data, &mylock](HDFSManager::InputFormat* input_format, int local_tid) {
    
    //DataObj this_obj;
    while (input_format->HasRecord()) {
      auto record = input_format->GetNextRecord();
      if (record.empty()) return;
      //this_obj = libsvm_parser(record);
      //data.push_back(std::string(record.begin(), record.end()));

      mylock.lock();    
      data.push_back(std::string(record.begin(), record.end()));
      mylock.unlock();

    }
    LOG(INFO) << data.size() << " lines in (node, thread):(" 
      << my_node.id << "," << local_tid << ")";
  });
  hdfs_manager.Stop();
  LOG(INFO) << "Finished loading data!";
  for (auto record: data) {
    LOG(INFO) << "read record = " << record;
  }
  // 1. Start engine
  Engine engine(my_node, nodes);
  engine.StartEverything();

  // 2. Create tables
  const int kTableId = 0;
  const int kMaxKey = 10000;
  const int kStaleness = 0;
  const int worker_num = 3;
  std::vector<third_party::Range> range;
  
  for (int i = 0; i < nodes.size() - 1; ++ i) {
    range.push_back({kMaxKey / nodes.size() * i, kMaxKey / nodes.size() * (i + 1)});
  }
  range.push_back({kMaxKey / nodes.size() * (nodes.size() - 1), kMaxKey});
  
  engine.CreateTable<float>(kTableId, range, 
      ModelType::SSP, StorageType::Map, kStaleness);
  engine.Barrier();
  
  // Load training data
  
  DataLoader data_loader("/home/ubuntu/Git_Project/flexps_bak/flexps/examples/gbdt/data/40_data_train.dat", "\t");
  const std::vector<std::vector<float>> & all_feat_vect_list = data_loader.get_feat_vect_list();
  const std::vector<float> & all_class_vect = data_loader.get_class_vect();
  

  // Find min and max for each feature
  std::vector<std::map<std::string, float>> _min_max_feat_list;

  for (int f_id = 0; f_id < all_feat_vect_list.size(); f_id++) {
    _min_max_feat_list.push_back(find_min_max(all_feat_vect_list[f_id]));
    //LOG(INFO) << "min = " << min_max_feat_list[f_id]["min"] << ", max = " << min_max_feat_list[f_id]["max"];
  }
  const std::vector<std::map<std::string, float>> & min_max_feat_list = _min_max_feat_list;

  // Find data size of dataset
  const int & data_num = all_feat_vect_list[0].size();
  

  // 3. Construct tasks
  MLTask task;
  std::vector<WorkerAlloc> worker_alloc;
  for (auto& node : nodes) {
    worker_alloc.push_back({node.id, worker_num});  // each node has worker_num workers
  }
  task.SetWorkerAlloc(worker_alloc);
  task.SetTables({kTableId});  // Use table 0
  task.SetLambda([kTableId, kMaxKey, worker_num, all_feat_vect_list, all_class_vect, min_max_feat_list, data_num](const Info& info){
    
    // Assign data to each worker
    int num_of_records_per_worker = data_num / worker_num;

    std::vector<std::vector<float>> feat_vect_list(all_feat_vect_list.size());
    std::vector<float> class_vect;

    if (info.worker_id == worker_num - 1) {
      class_vect.insert(class_vect.end(), all_class_vect.begin() + (info.worker_id * num_of_records_per_worker), all_class_vect.end());
      for (int i = 0; i < feat_vect_list.size(); i++) {
        feat_vect_list[i].insert(feat_vect_list[i].end(), all_feat_vect_list[i].begin() + (info.worker_id * num_of_records_per_worker), all_feat_vect_list[i].end());
      }
    }
    else {
      class_vect.insert(class_vect.end(), all_class_vect.begin() + (info.worker_id * num_of_records_per_worker), 
        all_class_vect.begin() + ((info.worker_id + 1) * num_of_records_per_worker));
      for (int i = 0; i < feat_vect_list.size(); i++) {
        feat_vect_list[i].insert(feat_vect_list[i].end(), all_feat_vect_list[i].begin() + (info.worker_id * num_of_records_per_worker),
          all_feat_vect_list[i].begin() + ((info.worker_id + 1) * num_of_records_per_worker));
      }
    }
    // Check log
    /*
    LOG(INFO) << "info.worker_id = " << info.worker_id;
    std::string class_str = "";
    for (int i = 0; i < class_vect.size(); i++) {
      class_str += std::to_string(class_vect[i]) + ", ";
    }
    LOG(INFO) << "class_str = " << class_str;

    std::string feat_str = "";
    for (int i = 0; i < feat_vect_list[5].size(); i++) {
      feat_str += std::to_string(feat_vect_list[5][i]) + ", ";
    }
    LOG(INFO) << "feat_str = " << feat_str;
    */

    // Step 0: Initialize setting
    auto table = info.CreateKVClientTable<float>(kTableId);
    //std::vector<Key> keys(kMaxKey);
    //std::iota(keys.begin(), keys.end(), 0);
    //std::vector<float> ret_vect;
    //int PS_key_ptr = 0;
    int feat_num = all_feat_vect_list.size(); // 7
    std::map<std::string, float> params = {
      {"num_of_trees", 6},
      {"max_depth", 3},
      //{"min_split_gain", 0.02},
      {"complexity_of_leaf", 0.05},
      {"rank_fraction", 0.1},
      {"total_data_num", (float) data_num}
    };

    std::map<std::string, std::string> options = {
      {"loss_function", "square_error"}
    };

    GBDTController controller;

    // Step 1: Load dataset


    // Initialize grad_vect, hess_vect
    controller.init(options, params);
    
    /* Check log
    for (int i = 0; i < grad_vect.size(); i++) {
      LOG(INFO) << "grad_vect[" << i << "] = " << grad_vect[i];
      LOG(INFO) << "hess_vect[" << i << "] = " << hess_vect[i];
    }
    */
    //GBDTTree tree = controller.build_tree(table, all_feat_vect_list, min_max_feat_list);
    controller.build_forest(table, all_class_vect, all_feat_vect_list, min_max_feat_list);

  });

  // 4. Run tasks
  //engine.Run(task);

  // 5. Stop engine
  engine.StopEverything();
}

}

int main(int argc, char** argv) {
  gflags::ParseCommandLineFlags(&argc, &argv, true);
  google::InitGoogleLogging(argv[0]);
  LOG(INFO) << "argv[0]: " << argv[0];
  flexps::Run();
}
