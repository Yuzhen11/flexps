#pragma once

#include <algorithm>
#include <memory>
#include <vector>

#include "base/node.hpp"
#include "base/node_util.hpp"
#include "comm/sender.hpp"
#include "comm/mailbox.hpp"
#include "driver/info.hpp"
#include "driver/ml_task.hpp"
#include "driver/simple_id_mapper.hpp"
#include "driver/worker_spec.hpp"
#include "server/asp_model.hpp"
#include "server/bsp_model.hpp"
#include "server/map_storage.hpp"
#include "server/server_thread.hpp"
#include "server/server_thread_group.hpp"
#include "server/ssp_model.hpp"
#include "server/vector_storage.hpp"
#include "worker/abstract_partition_manager.hpp"
#include "worker/app_blocker.hpp"
#include "worker/simple_range_manager.hpp"
#include "worker/worker_helper_thread.hpp"

// for sparsessp
#include "server/sparsessp/abstract_sparse_ssp_recorder.hpp"
#include "server/sparsessp/sparse_ssp_model.hpp"
#include "server/sparsessp/unordered_map_sparse_ssp_recorder.hpp"
#include "server/sparsessp/vector_sparse_ssp_recorder.hpp"

namespace flexps {

enum class ModelType { SSP, BSP, ASP, SparseSSP };
enum class StorageType { Map, Vector };
enum class SparseSSPRecorderType { None, Map, Vector };

/*
 * KVEngine handles the kvstore module
 */
class KVEngine {
 public:
  KVEngine(const Node& node, const std::vector<Node>& nodes, 
          SimpleIdMapper* const id_mapper, Mailbox* const mailbox) 
      : node_(node), nodes_(nodes), id_mapper_(id_mapper), mailbox_(mailbox) {}

  void StartKVEngine(int num_server_threads_per_node = 1);
  void StartServerThreads();
  void StartWorkerHelperThreads();
  void StartSender();

  void StopKVEngine();
  void StopServerThreads();
  void StopWorkerHelperThreads();
  void StopSender();

  template <typename Val>
  void CreateTable(uint32_t table_id, const std::vector<third_party::Range>& ranges, ModelType model_type,
                   StorageType storage_type, int model_staleness = 0);

  // Create SparseSSP Table, for testing sparsessp use only.
  template <typename Val>
  void CreateSparseSSPTable(uint32_t table_id, const std::vector<third_party::Range>& ranges, ModelType model_type,
                   StorageType storage_type, int model_staleness = 0, int speculation = 0,
                   SparseSSPRecorderType sparse_ssp_recorder_type = SparseSSPRecorderType::None);

  void Run(const MLTask& task);
 private:
  WorkerSpec AllocateWorkers(const std::vector<WorkerAlloc>& worker_alloc);
  void RegisterRangePartitionManager(uint32_t table_id, const std::vector<third_party::Range>& ranges);
  void InitTable(uint32_t table_id, const std::vector<uint32_t>& worker_ids);

 private:
  std::map<uint32_t, std::unique_ptr<AbstractPartitionManager>> partition_manager_map_;
  // nodes
  Node node_;
  std::vector<Node> nodes_;

  SimpleIdMapper* const id_mapper_;  // not owned
  Mailbox* const mailbox_;  // not owned

  // Elements managed by KVEngine
  std::unique_ptr<Sender> sender_;
  // worker elements
  std::unique_ptr<AppBlocker> app_blocker_;
  std::unique_ptr<WorkerHelperThread> worker_helper_thread_;
  // server elements
  std::unique_ptr<ServerThreadGroup> server_thread_group_;
};

template <typename Val>
void KVEngine::CreateTable(uint32_t table_id, const std::vector<third_party::Range>& ranges, ModelType model_type,
                         StorageType storage_type, int model_staleness) {
  RegisterRangePartitionManager(table_id, ranges);
  CHECK(server_thread_group_);

  CHECK(id_mapper_);
  auto server_thread_ids = id_mapper_->GetAllServerThreads();
  CHECK_EQ(ranges.size(), server_thread_ids.size());

  for (auto& server_thread : *server_thread_group_) {
    std::unique_ptr<AbstractStorage> storage;
    std::unique_ptr<AbstractModel> model;
    // Set up storage
    if (storage_type == StorageType::Map) {
      storage.reset(new MapStorage<Val>());
    } else if (storage_type == StorageType::Vector) {
      auto it = std::find(server_thread_ids.begin(), server_thread_ids.end(), server_thread->GetServerId());
      storage.reset(new VectorStorage<Val>(ranges[it - server_thread_ids.begin()]));
    } else {
      CHECK(false) << "Unknown storage_type";
    }
    // Set up model
    if (model_type == ModelType::SSP) {
      model.reset(new SSPModel(table_id, std::move(storage), model_staleness, server_thread_group_->GetReplyQueue()));
    } else if (model_type == ModelType::BSP) {
      model.reset(new BSPModel(table_id, std::move(storage), server_thread_group_->GetReplyQueue()));
    } else if (model_type == ModelType::ASP) {
      model.reset(new ASPModel(table_id, std::move(storage), server_thread_group_->GetReplyQueue()));
    } else {
      CHECK(false) << "Unknown model_type";
    }
    server_thread->RegisterModel(table_id, std::move(model));
  }
}

template <typename Val>
void KVEngine::CreateSparseSSPTable(uint32_t table_id, const std::vector<third_party::Range>& ranges, ModelType model_type,
                         StorageType storage_type, int model_staleness, int speculation,
                         SparseSSPRecorderType sparse_ssp_recorder_type) {
  RegisterRangePartitionManager(table_id, ranges);
  CHECK(server_thread_group_);

  CHECK(id_mapper_);
  auto server_thread_ids = id_mapper_->GetAllServerThreads();
  CHECK_EQ(ranges.size(), server_thread_ids.size());

  for (auto& server_thread : *server_thread_group_) {
    std::unique_ptr<AbstractStorage> storage;
    std::unique_ptr<AbstractModel> model;
    // Set up storage
    if (storage_type == StorageType::Map) {
      storage.reset(new MapStorage<Val>());
    } else if (storage_type == StorageType::Vector) {
      auto it = std::find(server_thread_ids.begin(), server_thread_ids.end(), server_thread->GetServerId());
      storage.reset(new VectorStorage<Val>(ranges[it - server_thread_ids.begin()]));
    } else {
      CHECK(false) << "Unknown storage_type";
    }
    // Set up model
    CHECK(model_type == ModelType::SparseSSP);
    std::unique_ptr<AbstractSparseSSPRecorder> recorder;
    CHECK(sparse_ssp_recorder_type != SparseSSPRecorderType::None);
    if (sparse_ssp_recorder_type == SparseSSPRecorderType::Map) {
      recorder.reset(new UnorderedMapSparseSSPRecorder(model_staleness, speculation));
    } else if (sparse_ssp_recorder_type == SparseSSPRecorderType::Vector) {
      auto it = std::find(server_thread_ids.begin(), server_thread_ids.end(), server_thread->GetServerId());
      recorder.reset(
          new VectorSparseSSPRecorder(model_staleness, speculation, ranges[it - server_thread_ids.begin()]));
    } else {
      CHECK(false) << "Unknown recorder type";
    }
    model.reset(new SparseSSPModel(table_id, std::move(storage), std::move(recorder),
                                   server_thread_group_->GetReplyQueue(), model_staleness, speculation));
    server_thread->RegisterModel(table_id, std::move(model));
  }
}

}  // namespace flexps
