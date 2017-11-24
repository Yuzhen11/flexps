#pragma once

#include <algorithm>
#include <memory>
#include <vector>

#include "base/node.hpp"
#include "base/node_util.hpp"
#include "comm/mailbox.hpp"
#include "driver/simple_id_mapper.hpp"
#include "driver/kv_engine.hpp"

namespace flexps {

class Engine {
 public:
  Engine(const Node& node, const std::vector<Node>& nodes) : node_(node), nodes_(nodes) {}

  void StartEverything(int num_server_threads_per_node = 1);

  void StopEverything();

  void Barrier();

  template <typename Val>
  void CreateTable(uint32_t table_id, const std::vector<third_party::Range>& ranges, ModelType model_type,
                   StorageType storage_type, int model_staleness = 0, int speculation = 0,
                   SparseSSPRecorderType sparse_ssp_recorder_type = SparseSSPRecorderType::None);

  void Run(const MLTask& task);

 private:
  // nodes
  Node node_;
  std::vector<Node> nodes_;

  std::unique_ptr<SimpleIdMapper> id_mapper_;
  std::unique_ptr<Mailbox> mailbox_;
  std::unique_ptr<KVEngine> kv_engine_;
};

template <typename Val>
void Engine::CreateTable(uint32_t table_id, const std::vector<third_party::Range>& ranges, ModelType model_type,
                         StorageType storage_type, int model_staleness, int speculation,
                         SparseSSPRecorderType sparse_ssp_recorder_type) {
  CHECK(kv_engine_);
  kv_engine_->CreateTable<Val>(table_id, ranges, model_type, storage_type, model_staleness, speculation, sparse_ssp_recorder_type);
}

}  // namespace flexps
