#pragma once

#include "base/abstract_id_mapper.hpp"

#include <cinttypes>
#include <vector>
#include <map>
#include <set>

#include "base/node.hpp"

namespace flexps {

/*
 * node_id = tid % 1000
 */
class SimpleIdMapper : public AbstractIdMapper {
 public:
  SimpleIdMapper(Node node, const std::vector<Node>& nodes) : node_(node), nodes_(nodes) {}

  // TODO(yuzhen): Make sure there is no thread-safety issue between (the Mailbox::Send and the engine thread).
  virtual uint32_t GetNodeIdForThread(uint32_t tid) override;

  void Init();
  uint32_t AllocateWorkerThread(uint32_t node_id);
  void DeallocateWorkerThread(uint32_t node_id, uint32_t tid);
  std::vector<uint32_t> GetServerThreadsForId(uint32_t node_id);
  std::vector<uint32_t> GetWorkerHelperThreadsForId(uint32_t node_id);
  std::vector<uint32_t> GetWorkerThreadsForId(uint32_t node_id);

  static const uint32_t kMaxNodeId = 1000;
  static const uint32_t kMaxThreadsPerNode = 1000;
  // BgThreads include server threads and worker helper threads.
  // Their ids are [0, 100) for node id 0.
  static const uint32_t kMaxBgThreadsPerNode = 100;

 private:
  std::map<uint32_t, std::vector<uint32_t>> node2server_;
  std::map<uint32_t, std::vector<uint32_t>> node2worker_helper_;
  std::map<uint32_t, std::set<uint32_t>> node2worker_;

  Node node_;
  std::vector<Node> nodes_;
};

}  // namespace flexps
