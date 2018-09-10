#include "gflags/gflags.h"
#include "glog/logging.h"

#include "driver/engine.hpp"
#include "worker/kv_client_table.hpp"
#include "base/serialization.hpp"
#include "boost/utility/string_ref.hpp"
#include "io/coordinator.hpp"
#include "io/file_splitter.hpp"
#include "io/hdfs_assigner.hpp"
#include "io/lineinput.hpp"
#include <algorithm>
#include <cstdlib>
#include <chrono>
#include <ctime>
#include <thread>

DEFINE_int32(my_id, -1, "The process id of this program");
DEFINE_string(config_file, "", "The config file path");
DEFINE_string(hdfs_namenode, "", "The hdfs namenode hostname");
DEFINE_string(input,"","The hdfs input url");
DEFINE_int32(hdfs_namenode_port,-1,"The hdfs namenode port");

namespace flexps {

struct hdfs_info{
  std::string input;
  std::string master_host;
  std::string worker_host;
  std::string hdfs_namenode;
  int master_port;
  int worker_port;
  int hdfs_namenode_port;
};

void Run() {
  srand(0);
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
  hdfs_info config;
  config.input = FLAGS_input;
  config.worker_host = my_node.hostname;
  config.worker_port = my_node.port;
  config.master_port = 19817;
  config.master_host = nodes[0].hostname;
  config.hdfs_namenode = FLAGS_hdfs_namenode;
  config.hdfs_namenode_port = FLAGS_hdfs_namenode_port;
  zmq::context_t* zmq_context = new zmq::context_t(1);
  std::thread hdfs_main_thread;
  std::thread load_thread;
  if(my_node.id == 0){
     hdfs_main_thread = std::thread([config, zmq_context]{
       HDFSBlockAssigner hdfs_block_assigner(config.hdfs_namenode, config.hdfs_namenode_port, zmq_context, config.master_port);
       hdfs_block_assigner.Serve();
     });
  }
  Coordinator* coordinator = new Coordinator(my_node.id, config.worker_host, zmq_context, config.master_host, config.master_port);
  coordinator->serve();
    
  load_thread = std::thread([config, coordinator, nodes, my_node, zmq_context]{
    auto start_time = std::chrono::steady_clock::now();
    int proc_id = my_node.id;
    LineInputFormat infmt(config.input, nodes.size(), 0, coordinator, config.worker_host, config.hdfs_namenode,config.hdfs_namenode_port);
    bool success = true;
    int count = 0; 
    boost::string_ref record;
    while (true) {
      success = infmt.next(record);
      count++;
    if (success == false)
      break;
    }
    BinStream finish_signal;
    finish_signal << config.worker_host << my_node.id;
    coordinator->notify_master(finish_signal, 300);
    LOG(INFO) << "the number of lines of datasets is " << count;
    auto end_time = std::chrono::steady_clock::now();
    auto total_time = std::chrono::duration_cast<std::chrono::milliseconds>(end_time - start_time).count();
    LOG(INFO) << "total time: " << total_time << " ms on worker";
  });
  if(my_node.id == 0)
    hdfs_main_thread.join();
  load_thread.join();

}

}  // namespace flexps

int main(int argc, char** argv) {
  gflags::ParseCommandLineFlags(&argc, &argv, true);
  google::InitGoogleLogging(argv[0]);
  flexps::Run();
}
