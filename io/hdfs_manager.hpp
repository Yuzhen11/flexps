#pragma once

#include <thread>
#include "gflags/gflags.h"
#include "glog/logging.h"

#include "base/node.hpp"
#include "base/node_util.hpp"
#include "base/serialization.hpp"
#include "boost/utility/string_ref.hpp"
#include "io/coordinator.hpp"
#include "io/lineinput.hpp"

namespace flexps {

/*
 * A wrapper for loading files from hdfs
 * Assume Node0 must exist, and it also has another thread to be the hdfs_assinger
 *
 * HDFSManager::Config config;
 * config.input = ...
 * ...
 * HDFSManager hdfs_manager(my_node, nodes, config, zmq_context, num_threads_per_node);
 * hdfs_manager.Start();
 * hdfs_manager.Run([](HDFSManager::InputFormat* input_format, int local_tid) {
 *   while (input_format->HasRecord()) {
 *     auto record = input_format->GetNextRecord();
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
  struct InputFormat {
    LineInputFormat* infmt_;
    boost::string_ref record;
    InputFormat(const Config& config, Coordinator* coordinator, int num_threads) {
      infmt_ = new LineInputFormat(config.input, num_threads, 0, coordinator, config.worker_host, config.hdfs_namenode,
                                   config.hdfs_namenode_port);
    }

    bool HasRecord() {
      bool has_record = infmt_->next(record);
      return has_record;
    }

    boost::string_ref GetNextRecord() { return record; }
  };

  HDFSManager(Node node, const std::vector<Node>& nodes, const Config& config, zmq::context_t* zmq_context,
              int num_threads_per_node);
  void Start();
  void Run(const std::function<void(InputFormat*, int)>& func);
  void Stop();

 private:
  const Node node_;
  const std::vector<Node> nodes_;
  const Config config_;
  Coordinator* coordinator_;
  zmq::context_t* zmq_context_;
  const int num_threads_per_node_;
  std::thread hdfs_main_thread_;  // Only in Node0
};

}  // namespace flexps
