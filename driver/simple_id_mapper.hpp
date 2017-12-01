#pragma once

#include "base/abstract_id_mapper.hpp"

#include <cinttypes>
#include <vector>
#include <map>
#include <unordered_map>
#include <set>

#include "base/node.hpp"

namespace flexps {

/*
 * node_id = tid / 1000
 */
class SimpleIdMapper : public AbstractIdMapper {
 public:
  SimpleIdMapper(Node node, const std::vector<Node>& nodes) : node_(node), nodes_(nodes) {}

  // TODO(yuzhen): Make sure there is no thread-safety issue between (the Mailbox::Send and the engine thread).
  virtual uint32_t GetNodeIdForThread(uint32_t tid) override;

  void Init(int num_server_threads_per_node = 1);
  uint32_t AllocateWorkerThread(uint32_t node_id);
  void DeallocateWorkerThread(uint32_t node_id, uint32_t tid);

  std::vector<uint32_t> GetServerThreadsForId(uint32_t node_id);
  std::vector<uint32_t> GetWorkerHelperThreadsForId(uint32_t node_id);
  std::vector<uint32_t> GetWorkerThreadsForId(uint32_t node_id);

  std::vector<uint32_t> GetAllServerThreads();

  std::pair<std::vector<uint32_t>, std::unordered_map<uint32_t, uint32_t>> GetChannelThreads(uint32_t num_local_threads, uint32_t num_global_threads);
  void ReleaseChannelThreads();

  static const uint32_t kMaxNodeId = 1000;
  static const uint32_t kMaxThreadsPerNode = 1000;
  // BgThreads include server threads, worker helper threads and model init threads.
  // Their ids are [0, 100) for node id 0.
  static const uint32_t kMaxBgThreadsPerNode = 100;
  // The server thread id for node 0 is in [0, 20)
  // The worker helper thread's id for node id 0 is in [20, 50)
  // The channel thread id for node id 0 is in [50, 100)
  static const uint32_t kWorkerHelperThreadId = 20;
  static const uint32_t kChannelThreadId = 50;

 private:
  // The server thread's id in each node
  std::map<uint32_t, std::vector<uint32_t>> node2server_;
  std::map<uint32_t, std::vector<uint32_t>> node2worker_helper_;
  std::map<uint32_t, uint32_t> node2model_init_;
  std::map<uint32_t, std::set<uint32_t>> node2worker_;

  // A flag to check whether channel threads have been allocated.
  // For simplicity, now the channel threads can only be allocated ONCE!
  // Remember to call ReleaseChannelThreads() when finish using the channel.
  bool channel_thread_allocated = false;

  Node node_;
  std::vector<Node> nodes_;
};

}  // namespace flexps
