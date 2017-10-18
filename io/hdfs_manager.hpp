#pragma once

#include "gflags/gflags.h"
#include "glog/logging.h"

#include "base/serialization.hpp"
#include "boost/utility/string_ref.hpp"
#include "io/coordinator.hpp"
#include "io/file_splitter.hpp"
#include "io/hdfs_assigner.hpp"
#include "io/lineInput.hpp"

namespace flexps {

/*
 * A wrapper for loading files from hdfs
 * Assume Node0 must exist, and it also has another thread to be the hdfs_assinger
 *
 * HDFSManager::Config config;
 * config.input = ...
 * ...
 * HDFSManager hdfs_manager(my_node, nodes, config, zmq_context);
 * hdfs_manager.Start();
 * TODO: Need to figure a way to support multiple threads friendly, the hdfs_manager should
 * know the number of loading thread beforehand so that it can tell the hdfs_assigner
 * std::thread th([](){  
 *   while (hdfs_manager.HasRecord()) {
 *     auto record = hdfs_manager.NextRecord();
 *   }
 * });
 * hdfs_manager.Stop();
 */
class HDFSManager {
 public:
  struct Config {
    std::string input;
    std::string master_host;
    std::string worker_host;
    std::string hdfs_namenode;
    int master_port;
    int worker_port;
    int hdfs_namenode_port;
  };
  HDFSManager(Node node, const std::vector<Node>& nodes, const Config& config, zmq::context_t* zmq_context)
    : node_(node), nodes_(nodes), config_(config), zmq_context_(zmq_context_) {
    // TODO: Check there must be node0 in nodes
  }
  void Start() {
    if (node_.id == 0) {
      hdfs_main_thread = std::thread([this]{
        HDFSBlockAssigner hdfs_block_assigner(config.hdfs_namenode, config.hdfs_namenode_port, zmq_context, config.master_port);
        hdfs_block_assigner.Serve();
      });
    }
    // TODO
    coordinator_.reset(new ...);
  }
  // TODO
  // Try to wrap all the logic inside
  // The user should use a much simpler interface like:
  // while (hdfs_manager.HasRecord()) {
  //   boost::string_ref record = hdfs_manager.GetNextRecord();
  // }
  bool HasRecord() {
  }
  boost::string_ref GetNextRecord() {
  }
  void Stop() {
    // TODO use coordinator to send finish_signal
    //
    if (hdfs_main_thread.joinable()) {  // join only for node 0
      hdfs_main_thread.join();
    }
  }
 private:
  Node node_;
  std::vector<Node> nodes_;
  Config config_;
  std::unique_ptr<Coordinator> coordinator_;
  zmq::context_t* zmq_context_;

  std::thread hdfs_assigner_thread_;  // Only in Node0
};

}  // namespace flexps
