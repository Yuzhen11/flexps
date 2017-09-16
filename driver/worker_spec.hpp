#pragma once

#include <map>
#include <vector>

#include "glog/logging.h"

namespace flexps {

/*
 * node_id: 0, 2, 3
 * worker_id for this task: 0, 1, 2 .. consecutive
 * thread_id: global worker thread id
 */
class WorkerSpec {
 public:
  // {{0, 3}, {1, 2}}: 3 workers on node 0, 2 workers on node 1.
  WorkerSpec() = default;
  WorkerSpec(const std::vector<std::pair<uint32_t, uint32_t>>& node_workers) {
    Init(node_workers);
  }
  bool HasLocalWorkers(uint32_t node_id) const {
    return node_to_workers_.find(node_id) != node_to_workers_.end();
  }
  const std::vector<uint32_t>& GetLocalWorkers(uint32_t node_id) const {
    auto it = node_to_workers_.find(node_id);
    CHECK(it != node_to_workers_.end());
    return it->second;
  }
  const std::vector<uint32_t>& GetLocalThreads(uint32_t node_id) const {
    auto it = node_to_threads_.find(node_id);
    CHECK(it != node_to_threads_.end());
    return it->second;
  }

  std::map<uint32_t, std::vector<uint32_t>> GetNodeToWorkers() {
    return node_to_workers_;
  }

  std::vector<uint32_t> GetThreadIds() {
    return {thread_ids_.begin(), thread_ids_.end()};
  }

  void InsertWorkerIdThreadId(uint32_t worker_id, uint32_t thread_id) {
    CHECK(worker_to_thread_.find(worker_id) == worker_to_thread_.end());
    CHECK(thread_to_worker_.find(thread_id) == thread_to_worker_.end());
    CHECK(thread_ids_.find(thread_id) == thread_ids_.end());
    CHECK(worker_to_node_.find(worker_id) != worker_to_node_.end());
    worker_to_thread_[worker_id] = thread_id;
    thread_to_worker_[thread_id] = worker_id;
    thread_ids_.insert(thread_id);
    node_to_threads_[worker_to_node_[worker_id]].push_back(thread_id);
  }
 private:
  void Init(const std::vector<std::pair<uint32_t, uint32_t>>& node_workers) {
    uint32_t worker_count = 0;
    for (const auto& kv : node_workers) {
      for (int i = 0; i < kv.second; ++ i) {
        worker_to_node_[num_workers] = kv.first;
        node_to_workers_[kv.first].push_back(num_workers);
        num_workers += 1;
      }
    }
  }

  // {0,0}, {1,0}, {2,0}, {3,1}, {4,1}
  std::map<uint32_t, uint32_t> worker_to_node_;
  // {0, {0,1,2}}, {1, {3,4}}
  std::map<uint32_t, std::vector<uint32_t>> node_to_workers_;

  std::map<uint32_t, uint32_t> worker_to_thread_;
  std::map<uint32_t, uint32_t> thread_to_worker_;
  std::map<uint32_t, std::vector<uint32_t>> node_to_threads_;
  std::set<uint32_t> thread_ids_;
  uint32_t num_workers = 0;
};

}  // namespace flexps
