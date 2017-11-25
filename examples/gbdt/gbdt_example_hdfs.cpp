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
#include <cstdlib>
// GBDT src
#include "examples/gbdt/src/data_loader.hpp"
#include "examples/gbdt/src/gbdt_controller.hpp"
#include "examples/gbdt/src/math_tools.hpp"
#include "examples/gbdt/src/util.hpp"

DEFINE_int32(my_id, -1, "The process id of this program");
DEFINE_string(config_file, "", "The config file path");
DEFINE_int32(num_workers_per_node, 1, "The number of workers per node");
DEFINE_string(run_mode, "", "The running mode of application");

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
  // TODO: include hdfs_manager.hpp
  
  HDFSManager::Config config;
  config.input = FLAGS_cluster_train_input;
  config.worker_host = my_node.hostname;
  config.worker_port = my_node.port;
  config.master_port = 19715;
  config.master_host = nodes[0].hostname;
  config.hdfs_namenode = FLAGS_hdfs_namenode;
  config.hdfs_namenode_port = FLAGS_hdfs_namenode_port;
  config.num_local_load_thread = FLAGS_num_workers_per_node;
  
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

  //for (auto record: data) {
  //  LOG(INFO) << "read record = " << record;
  //}
  
  
  // Use DataLoader to convert string to feat vect
  //std::vector<std::string> data;
  //data.push_back("0  1:8 2:350 3:150 4:4699 5:14.5 6:74 7:0");
  //data.push_back("1  1:4 2:97 3:78 4:1940 5:14.5 6:77 7:1");
  std::vector<float> all_class_vect;
  std::vector<std::vector<float>> all_feat_vect_list;
  DataLoader::read_hdfs_to_class_feat_vect(data, all_class_vect, all_feat_vect_list);
  const int & data_num = all_feat_vect_list[0].size();
  // Check log
  //for (float val: all_class_vect) {
  //   LOG(INFO) << "class val = " <<  val;
  //}
  //for (std::vector<float> feat_vect: all_feat_vect_list) {
  //  for (float val: feat_vect) {
  //    LOG(INFO) << "feat val = " << val;
  //  }
  //}

  // ad-hoc solution: hard code max-min range
  std::string maxmin_string = "";
  if (FLAGS_cluster_train_input.find("40") != std::string::npos) {
    maxmin_string = "4.0,8.0 79.0,400.0 60.0,175.0 1760.0,4746.0 12.0,19.5 71.0,82.0 0.0,2.0";
  }
  else if (FLAGS_cluster_train_input.find("cadata") != std::string::npos) {
    maxmin_string = "0.4999,15.0001 1.0,52.0 2.0,39320.0 1.0,6445.0 3.0,35682.0 1.0,6082.0 32.54,41.95 -124.35,-114.31";
  }
  else if (FLAGS_cluster_train_input.find("MSD") != std::string::npos) {
    maxmin_string = "1.749,61.970139 -337.092499,384.065735 -301.005066,322.85144 -154.183578,289.527435 -181.953369,262.068878 -81.794289,119.81559 -188.214005,172.402679 -72.503853,105.210281 -126.479042,146.297943 -41.63166,60.345348 -69.68087,30.833549 -94.041962,87.913239 0.13283,549.764893 8.4742,65735.78125 21.21435,36816.789062 17.857901,31849.486328 12.15042,19865.931641 5.51771,16831.949219 19.808809,10667.728516 6.25487,9569.77832 6.18376,9616.615234 15.3075,3721.873291 6.11644,6737.121582 5.17734,9813.233398 -2821.430176,2049.604248 -13374.757812,24479.664062 -12017.088867,14505.341797 -4324.864746,3410.615479 -3357.279785,3277.633789 -3115.374512,3553.184814 -3805.66626,1828.028442 -1516.356445,1954.355469 -1679.118286,2887.846191 -1590.637085,2330.33374 -989.645813,1813.236572 -1711.484009,2496.122559 -8448.195312,14148.998047 -10095.725586,8059.146484 -9803.758789,6065.054688 -7882.823242,8360.145508 -4673.355469,3537.503662 -4175.412598,3892.124756 -4975.381836,1202.491577 -1072.955566,1830.544678 -1021.289185,746.70752 -1329.959717,1198.626709 -14861.695312,9059.759766 -3992.688721,5819.494629 -6642.399414,4126.704102 -2344.526611,2021.697754 -2270.811035,1426.848022 -1746.478271,2460.43335 -3188.17749,2394.662354 -2199.782227,2900.520264 -1694.260376,569.135803 -4536.699707,6955.414551 -5111.601562,8889.25 -4730.599121,13001.258789 -3017.925293,5419.276855 -2499.95459,5690.291504 -1900.104858,1811.228638 -1129.513428,973.052979 -583.200989,812.424255 -10345.833008,11048.198242 -7375.977539,2877.738525 -3896.275146,3447.479004 -1199.004395,2055.039551 -2564.788086,4779.800293 -1904.984375,5286.821777 -930.326233,745.503418 -7057.712402,3958.070068 -6953.357422,4741.181152 -8400.603516,2124.10083 -1812.889404,1639.93042 -1387.505493,1278.333374 -718.421021,741.028992 -9831.454102,10020.283203 -2019.431396,3423.595459 -8390.035156,4723.481934 -4754.937012,3735.025635 -437.722015,840.973389 -4402.376465,4469.455078 -1810.689209,3210.70166 -3098.350342,1672.647095 -341.789124,260.544891 -3168.924561,3662.065674 -4319.992188,2833.608887 -236.039261,463.419495 -7458.37793,7393.398438 -318.223328,600.766235";
  }
  else {
    LOG(INFO) << "Dataset not found";
    return;
  }

  std::vector<std::string> maxmin_element_vect = split(maxmin_string, " ");

  // convert string to float
  std::vector<std::map<std::string, float>> _min_max_feat_list;
  for (std::string maxmin_element: maxmin_element_vect) {
    std::map<std::string, float> maxmin_map;
    std::vector<std::string> maxmin_vect = split(maxmin_element, ",");
    maxmin_map["min"] = atof(maxmin_vect[0].c_str());
    maxmin_map["max"] = atof(maxmin_vect[1].c_str());
    _min_max_feat_list.push_back(maxmin_map);
  }
  const std::vector<std::map<std::string, float>> & min_max_feat_list = _min_max_feat_list;
  //for (std::map<std::string, float> min_max_feat: _min_max_feat_list) {
  //  LOG(INFO) << "min = " << min_max_feat["min"];
  //  LOG(INFO) << "max = " << min_max_feat["max"];
  //}

  // ad-hoc solution end
  

  // Load data (Local)
  /*
  DataLoader data_loader(FLAGS_local_train_input, "\t");
  const std::vector<std::vector<float>> & all_feat_vect_list = data_loader.get_feat_vect_list();
  const std::vector<float> & all_class_vect = data_loader.get_class_vect();
  LOG(INFO) << "Load data successfully";
  LOG(INFO) << "feat num = " << all_feat_vect_list.size();
  LOG(INFO) << "data size of feat = " << all_feat_vect_list[0].size();

  // Find min and max for each feature
  std::vector<std::map<std::string, float>> _min_max_feat_list;

  for (int f_id = 0; f_id < all_feat_vect_list.size(); f_id++) {
    _min_max_feat_list.push_back(find_min_max(all_feat_vect_list[f_id]));
    //LOG(INFO) << "min = " << _min_max_feat_list[f_id]["min"] << ", max = " << _min_max_feat_list[f_id]["max"];
  }
  const std::vector<std::map<std::string, float>> & min_max_feat_list = _min_max_feat_list;

  // Find data size of dataset
  const int & data_num = all_feat_vect_list[0].size();
  LOG(INFO) << "Find max min ok";
  */

  // 1. Start engine
  const int nodeId = my_node.id;
  Engine engine(my_node, nodes);
  engine.StartEverything();

  // 2. Create tables
  const int kTableId = 0;
  const int kMaxKey = 1000000;
  const int kStaleness = 0;
  const int worker_num = (int) FLAGS_num_workers_per_node;
  std::vector<third_party::Range> range;
  
  /*
  for (int i = 0; i < nodes.size() - 1; ++ i) {
    range.push_back({kMaxKey / nodes.size() * i, kMaxKey / nodes.size() * (i + 1)});
  }
  range.push_back({kMaxKey / nodes.size() * (nodes.size() - 1), kMaxKey});
  */
  range.push_back({0, kMaxKey});

  engine.CreateTable<float>(kTableId, range, 
      ModelType::SSP, StorageType::Map, kStaleness);
  engine.Barrier();
  
  // 3. Construct tasks
  MLTask task;
  std::vector<WorkerAlloc> worker_alloc;
  for (auto& node : nodes) {
    worker_alloc.push_back({node.id, worker_num});  // each node has worker_num workers
  }
  task.SetWorkerAlloc(worker_alloc);
  task.SetTables({kTableId});  // Use table 0
  task.SetLambda([kTableId, kMaxKey, nodeId, worker_num, all_feat_vect_list, all_class_vect, min_max_feat_list, data_num](const Info& info){
    //LOG(INFO) << "info.local_id = " << info.local_id << ", info.worker_id = " << info.worker_id << ", info.thread_id  = " << info.thread_id;
    std::vector<std::vector<float>> feat_vect_list(all_feat_vect_list.size());
    std::vector<float> class_vect;

    if (FLAGS_run_mode == "local") {
      // Assign data to each worker
      int num_of_records_per_worker = data_num / worker_num;

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
    }
    else {
      feat_vect_list = all_feat_vect_list;
      class_vect = all_class_vect;
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
    std::map<std::string, float> params = {
      {"num_of_trees", FLAGS_num_of_trees},
      {"max_depth", FLAGS_max_depth},
      //{"min_split_gain", 0.02},
      {"complexity_of_leaf", FLAGS_complexity_of_leaf},
      {"rank_fraction", FLAGS_rank_fraction},
      {"total_data_num", (float) data_num}
    };

    std::map<std::string, std::string> options = {
      {"loss_function", "square_error"}
    };

    if (nodeId == 0 && info.worker_id == 0) {
      options["show_result"] = "true";
    }

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
    controller.build_forest(table, class_vect, feat_vect_list, min_max_feat_list);

    // Show prediction result for test dataset
    if (options["show_result"] == "true") {
      std::vector<float> test_class_vect = all_class_vect;
      std::vector<std::vector<float>> test_feat_vect_list = all_feat_vect_list;
      controller.show_predict_result(test_feat_vect_list, test_class_vect);
    }
  });

  // 4. Run tasks
  engine.Run(task);

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