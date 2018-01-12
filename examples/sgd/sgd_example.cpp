#include "gflags/gflags.h"
#include "glog/logging.h"

#include <gperftools/profiler.h>
#include <algorithm>
#include <chrono>
#include <cstdlib>
#include <ctime>
#include <thread>

#include "boost/utility/string_ref.hpp"
#include "base/serialization.hpp"
#include "io/hdfs_manager.hpp"
#include "lib/libsvm_parser.cpp"
#include "driver/engine.hpp"
#include "worker/kv_client_table.hpp"
#include "examples/sgd/lib/objectives.hpp"
#include "examples/sgd/lib/optimizers.hpp"

DEFINE_int32(my_id, -1, "The process id of this program");
DEFINE_string(config_file, "", "The config file path");
DEFINE_string(hdfs_namenode, "", "The hdfs namenode hostname");
DEFINE_string(input, "", "The hdfs input url");
DEFINE_int32(hdfs_namenode_port, -1, "The hdfs namenode port");

DEFINE_uint64(num_dims, 1000, "number of dimensions");
DEFINE_int32(num_iters, 10, "number of iters");

DEFINE_string(kModelType, "", "ASP/SSP/BSP");
DEFINE_string(kStorageType, "", "Map/Vector");
DEFINE_int32(kStaleness, 0, "stalness");
DEFINE_uint32(num_workers_per_node, 1, "num_workers_per_node");
DEFINE_int32(num_servers_per_node, 1, "num_servers_per_node");
DEFINE_int32(batch_size, 100, "batch size of each epoch");
DEFINE_double(alpha, 0.1, "learning rate");
DEFINE_int32(report_interval, 100, "report interval");
DEFINE_int32(learning_rate_decay, 10, "learning rate decay");
DEFINE_string(trainer, "svm", "objective trainer");
DEFINE_double(lambda, 0.1, "lambda");

namespace flexps {

void Run() {
srand(0);
  CHECK_NE(FLAGS_my_id, -1);
  CHECK(!FLAGS_config_file.empty());
  CHECK(FLAGS_kModelType == "ASP" || FLAGS_kModelType == "BSP" || FLAGS_kModelType == "SSP");
  CHECK(FLAGS_kStorageType == "Map" || FLAGS_kStorageType == "Vector");
  CHECK_GT(FLAGS_num_dims, 0);
  CHECK_GT(FLAGS_num_iters, 0);
  CHECK_LE(FLAGS_kStaleness, 5);
  CHECK_GE(FLAGS_kStaleness, 0);
  CHECK_LE(FLAGS_num_workers_per_node, 20);
  CHECK_GE(FLAGS_num_workers_per_node, 1);
  CHECK_LE(FLAGS_num_servers_per_node, 20);
  CHECK_GE(FLAGS_num_servers_per_node, 1);

  if (FLAGS_my_id == 0) {
    LOG(INFO) << "Running in " << FLAGS_kModelType << " mode";
    LOG(INFO) << "num_dims: " << FLAGS_num_dims;
    LOG(INFO) << "num_iters: " << FLAGS_num_iters;
    LOG(INFO) << "num_workers_per_node: " << FLAGS_num_workers_per_node;
    LOG(INFO) << "num_servers_per_node: " << FLAGS_num_servers_per_node;
  }

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
    int count=0;  
    DataObj this_obj;
    while (input_format->HasRecord()) {
      auto record = input_format->GetNextRecord();
      if (record.empty()) return;
      this_obj = libsvm_parser(record);

      mylock.lock();    
      data.push_back(std::move(this_obj));
      mylock.unlock();
      count++;
    }
    LOG(INFO) << count << " lines in (node, thread):(" 
      << my_node.id << "," << local_tid << ")";
  });
  hdfs_manager.Stop();
  LOG(INFO) << "Finished loading data!";

  // 2. Start engine
  Engine engine(my_node, nodes);
  engine.StartEverything(FLAGS_num_servers_per_node);

  // 3. Create tables
  const int kTableId = 0;
  std::vector<third_party::Range> range;
  int num_total_servers = nodes.size() * FLAGS_num_servers_per_node;
  uint64_t num_features = FLAGS_num_dims + 1;
  for (int i = 0; i < num_total_servers - 1; ++ i) {
    range.push_back({num_features / num_total_servers * i, num_features / num_total_servers * (i + 1)});
  }
  range.push_back({num_features / num_total_servers * (num_total_servers - 1), num_features });
  ModelType model_type;
  if (FLAGS_kModelType == "ASP") {
    model_type = ModelType::ASP;
  } else if (FLAGS_kModelType == "SSP") {
    model_type = ModelType::SSP;
  } else if (FLAGS_kModelType == "BSP") {
    model_type = ModelType::BSP;
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

  engine.CreateTable<float>(kTableId, range, 
      model_type, storage_type, FLAGS_kStaleness);
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
    LOG(INFO) << "Hi";
    LOG(INFO) << info.DebugString();

    Objective* objective_ptr;
    if (FLAGS_trainer == "lr") {
            objective_ptr = new SigmoidObjective(FLAGS_num_dims + 1);// +1 for intercept
LOG(INFO) <<"LR";
        } else if (FLAGS_trainer == "lasso") {
            objective_ptr = new LassoObjective(FLAGS_num_dims + 1, FLAGS_lambda);
LOG(INFO) <<"lasso";
        } else {  // default svm
            objective_ptr = new SVMObjective(FLAGS_num_dims + 1, FLAGS_lambda);
LOG(INFO) <<"svm";
        }
    //objective_ptr = new SigmoidObjective(FLAGS_num_dims+1);
    SGDOptimizer sgd(objective_ptr, FLAGS_report_interval);

    OptimizerConfig conf;
    conf.num_iters = FLAGS_num_iters;
    conf.num_features = FLAGS_num_dims;
    conf.num_workers_per_node = FLAGS_num_workers_per_node;;
    conf.kTableId = 0;
    conf.alpha = FLAGS_alpha/FLAGS_num_workers_per_node;
    conf.batch_size = FLAGS_batch_size;
    conf.learning_rate_decay = FLAGS_learning_rate_decay;
    
    int iterOffset = 0; 
    sgd.train(info, data, conf, iterOffset);
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
