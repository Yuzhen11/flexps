#include "io/hdfs_manager.hpp"

#include "io/hdfs_assigner.hpp"
#include "gflags/gflags.h"
#include "glog/logging.h"

namespace flexps {

HDFSManager::HDFSManager(Node node, const std::vector<Node>& nodes, const Config& config, zmq::context_t* zmq_context)
    : node_(node),
      nodes_(nodes),
      config_(config),
      zmq_context_(zmq_context) {
  CHECK(!nodes.empty());
  CHECK(CheckValidNodeIds(nodes));
  CHECK(HasNode(nodes, 0));
  CHECK(config_.num_local_load_thread);
}
void HDFSManager::Start() {
  if (node_.id == 0) {
    hdfs_main_thread_ = std::thread([this] {
      HDFSBlockAssigner hdfs_block_assigner(config_.hdfs_namenode, config_.hdfs_namenode_port, zmq_context_,
                                            config_.master_port);
      hdfs_block_assigner.Serve();
    });
  }
}

void HDFSManager::Run(const std::function<void(InputFormat*, int)>& func) {
  int num_threads = nodes_.size() * config_.num_local_load_thread;
  coordinator_ = new Coordinator(node_.id, config_.worker_host, zmq_context_, config_.master_host, config_.master_port);
  coordinator_->serve();
  std::vector<std::thread> threads;
  for (int i = 0; i < config_.num_local_load_thread; ++i) {
    std::thread load_thread = std::thread([this, num_threads, i, func] {
      InputFormat input_format(config_, coordinator_, num_threads);
      func(&input_format, i);
      BinStream finish_signal;
      LOG(INFO) << "Send finish signal";
      finish_signal << config_.worker_host << node_.id * config_.num_local_load_thread + i;
      coordinator_->notify_master(finish_signal, 300);
    });
    threads.push_back(std::move(load_thread));
  }
  for (int i = 0; i < config_.num_local_load_thread; ++i) {
    threads[i].join();
  }
}

void HDFSManager::Stop() {
  if (node_.id == 0) {  // join only for node 0
    hdfs_main_thread_.join();
  }
}

}  // namespace flexps
