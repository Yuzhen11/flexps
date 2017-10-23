#pragma once

#include <thread>
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
              int num_threads_per_node)
      : node_(node),
        nodes_(nodes),
        config_(config),
        zmq_context_(zmq_context),
        num_threads_per_node_(num_threads_per_node) {
    CHECK(!nodes.empty()) << "not a valid node group";
  }
  void Start() {
    if (node_.id == 0) {
      hdfs_main_thread_ = std::thread([this] {
        HDFSBlockAssigner hdfs_block_assigner(config_.hdfs_namenode, config_.hdfs_namenode_port, zmq_context_,
                                              config_.master_port);
        hdfs_block_assigner.Serve();
      });
    }
  }

  void Run(const std::function<void(InputFormat*)>& func) {
    int num_threads = nodes_.size() * num_threads_per_node_;
    coordinator_ =
        new Coordinator(node_.id, config_.worker_host, zmq_context_, config_.master_host, config_.master_port);
    coordinator_->serve();
    std::vector<std::thread> threads;
    for (int i = 0; i < num_threads_per_node_; ++i) {
      std::thread load_thread = std::thread([this, num_threads, i, func] {
        InputFormat input_format(config_, coordinator_, num_threads);
        func(&input_format);
        BinStream finish_signal;
        LOG(INFO) << "Send finish signal";
        finish_signal << config_.worker_host << node_.id * num_threads_per_node_ + i;
        coordinator_->notify_master(finish_signal, 300);
      });
      threads.push_back(std::move(load_thread));
    }
    for (int i = 0; i < num_threads_per_node_; ++i) {
      threads[i].join();
    }
  }

  void Stop() {
    if (node_.id == 0) {  // join only for node 0
      hdfs_main_thread_.join();
    }
  }

 private:
  Node node_;
  std::vector<Node> nodes_;
  const Config config_;
  Coordinator* coordinator_;
  zmq::context_t* zmq_context_;
  zmq::context_t context;
  int num_threads_per_node_;
  std::thread hdfs_main_thread_;  // Only in Node0
};

}  // namespace flexps
