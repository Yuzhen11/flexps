#pragma once

#include "driver/ml_task.hpp"

#include <map>
#include <vector>

namespace flexps {

/*
 * node_id: 0, 1
 * worker_id for this task: 0, 1, 2 .. consecutive
 * thread_id: global worker thread id
 */
class WorkerSpec {
 public:
  // {{0, 3}, {1, 2}}: 3 workers on node 0, 2 workers on node 1.
  WorkerSpec() = delete;
  WorkerSpec(const std::vector<WorkerAlloc>& worker_alloc);
  bool HasLocalWorkers(uint32_t node_id) const;
  const std::vector<uint32_t>& GetLocalWorkers(uint32_t node_id) const;
  const std::vector<uint32_t>& GetLocalThreads(uint32_t node_id) const;

  std::map<uint32_t, std::vector<uint32_t>> GetNodeToWorkers();
  std::vector<uint32_t> GetThreadIds();

  void InsertWorkerIdThreadId(uint32_t worker_id, uint32_t thread_id);

  static const uint32_t kMaxNodeId = 1000;
  static const uint32_t kMaxThreadsPerNode = 1000;
  // BgThreads include server threads, worker helper threads and model init threads.
  static const uint32_t kMaxBgThreadsPerNode = 100;

 private:
  void Init(const std::vector<WorkerAlloc>& worker_alloc);

  // {0,0}, {1,0}, {2,0}, {3,1}, {4,1}
  std::map<uint32_t, uint32_t> worker_to_node_;
  // {0, {0,1,2}}, {1, {3,4}}
  std::map<uint32_t, std::vector<uint32_t>> node_to_workers_;

  // {0, 100}, {1, 101}, {2, 102}, {3, 1100}, {4, 1101}
  std::map<uint32_t, uint32_t> worker_to_thread_;
  std::map<uint32_t, uint32_t> thread_to_worker_;

  // {0,100}, {0,101}, {0,102}, {1,1100}, {1,1101}
  std::map<uint32_t, std::vector<uint32_t>> node_to_threads_;
  // {100, 101, 102, 1100, 1101}
  std::set<uint32_t> thread_ids_;
  uint32_t num_workers = 0;
};

}  // namespace flexps
