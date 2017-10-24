#include "gflags/gflags.h"
#include "glog/logging.h"

#include <gperftools/profiler.h>

#include <algorithm>
#include <chrono>
#include <cstdlib>
#include <ctime>
#include <thread>
#include "base/serialization.hpp"
#include "boost/utility/string_ref.hpp"
#include "driver/engine.hpp"
#include "driver/node_parser.hpp"
#include "io/hdfs_manager.hpp"
#include "worker/kv_client_table.hpp"

DEFINE_int32(my_id, -1, "The process id of this program");
DEFINE_string(config_file, "", "The config file path");
DEFINE_string(hdfs_namenode, "", "The hdfs namenode hostname");
DEFINE_string(input, "", "The hdfs input url");
DEFINE_int32(hdfs_namenode_port, -1, "The hdfs namenode port");

namespace flexps {

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
  HDFSManager::Config config;
  config.input = FLAGS_input;
  config.worker_host = my_node.hostname;
  config.worker_port = my_node.port;
  config.master_port = 19717;
  config.master_host = nodes[0].hostname;
  config.hdfs_namenode = FLAGS_hdfs_namenode;
  config.hdfs_namenode_port = FLAGS_hdfs_namenode_port;
  zmq::context_t* zmq_context = new zmq::context_t(1);
  int num_threads_per_node = 2;
  HDFSManager hdfs_manager(my_node, nodes, config, zmq_context, num_threads_per_node);
  LOG(INFO) << "manager set up";
  hdfs_manager.Start();
  LOG(INFO) << "manager start";
  hdfs_manager.Run([](HDFSManager::InputFormat* input_format) {
    // hdfs_manager.Run();
    int count = 0;
    while (input_format->HasRecord()) {
      auto record = input_format->GetNextRecord();
      count++;
    }
    LOG(INFO) << "the number of lines of datasets is " << count;
  });
  hdfs_manager.Stop();
}

}  // namespace flexps

int main(int argc, char** argv) {
  gflags::ParseCommandLineFlags(&argc, &argv, true);
  google::InitGoogleLogging(argv[0]);
  flexps::Run();
}
