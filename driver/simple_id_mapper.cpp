#include "driver/simple_id_mapper.hpp"

#include "glog/logging.h"

namespace flexps {

const uint32_t SimpleIdMapper::kMaxNodeId;
const uint32_t SimpleIdMapper::kMaxThreadsPerNode;
const uint32_t SimpleIdMapper::kMaxBgThreadsPerNode;

void SimpleIdMapper::Init() {
  // Suppose there are 1 server and 1 worker_helper_thread for each node
  for (const auto& node : nodes_) {
    CHECK_LT(node.id, kMaxNodeId);
    // {0, 1000, 2000, ...} are server threads
    node2server_[node.id].push_back(node.id * kMaxThreadsPerNode);
    // {1, 1001, 2001, ...} are worker helper threads
    node2worker_helper_[node.id].push_back(node.id * kMaxThreadsPerNode + 1);
  }
}

uint32_t SimpleIdMapper::AllocateWorkerThread(uint32_t node_id) {
  CHECK_LT(node2worker_[node_id].size(), kMaxThreadsPerNode - kMaxBgThreadsPerNode);
  for (int i = kMaxBgThreadsPerNode; i < kMaxThreadsPerNode; ++ i) {
    int tid = i + node_id * kMaxThreadsPerNode;
    if (node2worker_[node_id].find(tid) == node2worker_[node_id].end()) {
      node2worker_[node_id].insert(tid);
      return tid;
    }
  }
}

void SimpleIdMapper::DeallocateWorkerThread(uint32_t node_id, uint32_t tid) {
  CHECK(node2worker_[node_id].find(tid) != node2worker_[node_id].end());
  node2worker_[node_id].erase(tid);
}

uint32_t SimpleIdMapper::GetNodeIdForThreads(uint32_t tid) {
  return tid / kMaxThreadsPerNode;
}

std::vector<uint32_t> SimpleIdMapper::GetServerThreadsForId(uint32_t node_id) {
  return node2server_[node_id];
}

std::vector<uint32_t> SimpleIdMapper::GetWorkerHelperThreadsForId(uint32_t node_id) {
  return node2worker_helper_[node_id];
}

std::vector<uint32_t> SimpleIdMapper::GetWorkerThreadsForId(uint32_t node_id) {
  std::vector<uint32_t> ret(node2worker_[node_id].begin(), node2worker_[node_id].end());
  return ret;
}

}  // namespace flexps
