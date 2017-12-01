#include "driver/simple_id_mapper.hpp"

#include "glog/logging.h"

namespace flexps {

const uint32_t SimpleIdMapper::kMaxNodeId;
const uint32_t SimpleIdMapper::kMaxThreadsPerNode;
const uint32_t SimpleIdMapper::kMaxBgThreadsPerNode;
const uint32_t SimpleIdMapper::kWorkerHelperThreadId;
const uint32_t SimpleIdMapper::kChannelThreadId;

void SimpleIdMapper::Init(int num_server_threads_per_node) {
  CHECK_GT(num_server_threads_per_node, 0);
  CHECK_LE(num_server_threads_per_node, kWorkerHelperThreadId);
  // Suppose there are 1 server and 1 worker_helper_thread for each node
  for (const auto& node : nodes_) {
    CHECK_LT(node.id, kMaxNodeId);
    // {0, 1000, 2000, ...} are server threads if num_server_threads_per_node is 1
    for (int i = 0; i < num_server_threads_per_node; ++ i) {
      node2server_[node.id].push_back(node.id * kMaxThreadsPerNode + i);
    }
    // Only 1 worker helper thread now
    // {10, 1010, 2010, ...} are worker helper threads
    node2worker_helper_[node.id].push_back(node.id * kMaxThreadsPerNode + kWorkerHelperThreadId);
  }
}

uint32_t SimpleIdMapper::AllocateWorkerThread(uint32_t node_id) {
  CHECK(node2worker_helper_.find(node_id) != node2worker_helper_.end());
  CHECK_LE(node2worker_[node_id].size(), kMaxThreadsPerNode - kMaxBgThreadsPerNode);
  for (int i = kMaxBgThreadsPerNode; i < kMaxThreadsPerNode; ++ i) {
    int tid = i + node_id * kMaxThreadsPerNode;
    if (node2worker_[node_id].find(tid) == node2worker_[node_id].end()) {
      node2worker_[node_id].insert(tid);
      return tid;
    }
  }
  CHECK(false);
  return -1;
}

void SimpleIdMapper::DeallocateWorkerThread(uint32_t node_id, uint32_t tid) {
  CHECK(node2worker_helper_.find(node_id) != node2worker_helper_.end());
  CHECK(node2worker_[node_id].find(tid) != node2worker_[node_id].end());
  node2worker_[node_id].erase(tid);
}

uint32_t SimpleIdMapper::GetNodeIdForThread(uint32_t tid) {
  return tid / kMaxThreadsPerNode;
}

std::vector<uint32_t> SimpleIdMapper::GetServerThreadsForId(uint32_t node_id) {
  return node2server_[node_id];
}

std::vector<uint32_t> SimpleIdMapper::GetWorkerHelperThreadsForId(uint32_t node_id) {
  return node2worker_helper_[node_id];
}

std::vector<uint32_t> SimpleIdMapper::GetWorkerThreadsForId(uint32_t node_id) {
  return {node2worker_[node_id].begin(), node2worker_[node_id].end()};
}

std::vector<uint32_t> SimpleIdMapper::GetAllServerThreads() {
  std::vector<uint32_t> ret;
  for (auto& kv : node2server_) {
    ret.insert(ret.end(), kv.second.begin(), kv.second.end());
  }
  return ret;
}

std::pair<std::vector<uint32_t>, std::unordered_map<uint32_t, uint32_t>> SimpleIdMapper::GetChannelThreads(uint32_t num_local_threads, uint32_t num_global_threads) {
  CHECK_EQ(num_local_threads*nodes_.size(), num_global_threads);
  CHECK_LT(num_local_threads, kMaxBgThreadsPerNode - kChannelThreadId);
  channel_thread_allocated = true;
  std::pair<std::vector<uint32_t>, std::unordered_map<uint32_t, uint32_t>> ret;
  int c = 0;
  for (const auto& node : nodes_) {
    for (int i = 0; i < num_local_threads; ++ i) {
      ret.second.insert({c, node.id*kMaxThreadsPerNode+kChannelThreadId+i});
      if (node.id == node_.id) {
        ret.first.push_back(c);
      }
      c += 1;
    }
  }
  CHECK_EQ(ret.first.size(), num_local_threads);
  return ret;
}

void SimpleIdMapper::ReleaseChannelThreads() {
  CHECK(channel_thread_allocated);
  channel_thread_allocated = false;
}

}  // namespace flexps
