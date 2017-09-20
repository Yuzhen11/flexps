#include "driver/worker_spec.hpp"
#include "glog/logging.h"

namespace flexps {

  const uint32_t WorkerSpec::kMaxNodeId;
  const uint32_t WorkerSpec::kMaxThreadsPerNode;
  const uint32_t WorkerSpec::kMaxBgThreadsPerNode;

  WorkerSpec::WorkerSpec(const std::vector<WorkerAlloc>& worker_alloc) {
    Init(worker_alloc);
  }
  bool WorkerSpec::HasLocalWorkers(uint32_t node_id) const {
    return node_to_workers_.find(node_id) != node_to_workers_.end();
  }
  const std::vector<uint32_t>& WorkerSpec::GetLocalWorkers(uint32_t node_id) const {
    auto it = node_to_workers_.find(node_id);
    CHECK(it != node_to_workers_.end());
    return it->second;
  }
  const std::vector<uint32_t>& WorkerSpec::GetLocalThreads(uint32_t node_id) const {
    auto it = node_to_threads_.find(node_id);
    CHECK(it != node_to_threads_.end());
    return it->second;
  }

  std::map<uint32_t, std::vector<uint32_t>> WorkerSpec::GetNodeToWorkers() {
    return node_to_workers_;
  }

  std::vector<uint32_t> WorkerSpec::GetThreadIds() {
    return {thread_ids_.begin(), thread_ids_.end()};
  }

  void WorkerSpec::InsertWorkerIdThreadId(uint32_t worker_id, uint32_t thread_id) {
    CHECK(worker_to_thread_.find(worker_id) == worker_to_thread_.end());
    CHECK(thread_to_worker_.find(thread_id) == thread_to_worker_.end());
    CHECK(thread_ids_.find(thread_id) == thread_ids_.end());
    CHECK(worker_to_node_.find(worker_id) != worker_to_node_.end());
    worker_to_thread_[worker_id] = thread_id;
    thread_to_worker_[thread_id] = worker_id;
    thread_ids_.insert(thread_id);
    node_to_threads_[worker_to_node_[worker_id]].push_back(thread_id);
  }

  void WorkerSpec::Init(const std::vector<WorkerAlloc>& worker_alloc) {
    uint32_t worker_count = 0;
    for (const auto& node : worker_alloc) {
      CHECK_LT(node.node_id, kMaxNodeId);
      CHECK_LT(node.num_workers, kMaxThreadsPerNode - kMaxBgThreadsPerNode);
      for (int i = 0; i < node.num_workers; ++ i) {
        worker_to_node_[num_workers] = node.node_id;
        node_to_workers_[node.node_id].push_back(num_workers);
        num_workers += 1;
      }
    }
  }
}  // namespace flexps
