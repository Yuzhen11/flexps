#pragma once

#include "kv_client_table.hpp"
#include "kv_table_box.hpp"

#include "worker/abstract_callback_runner.hpp"
#include "worker/abstract_partition_manager.hpp"

namespace flexps {
/*
 * Get (optional) -> Add (optional) -> Clock ->
 *
 * It is chunk-based version of KVClientTable
 *
 */                                                                                                                 

template <typename Val>
class KVChunkClientTable: public KVClientTable<Val> {
 public:
  using KVClientTable<Val>::KVClientTable;
  using KVClientTable<Val>::kv_table_box_;
  using KVClientTable<Val>::callback_runner_;
  using SlicedKVs = AbstractPartitionManager::SlicedKVs;
  // The chunk version, it will tranform 2-dimension vector to 1-dimension one
  void AddChunk(const std::vector<Key>& keys, const std::vector<std::vector<Val>>& chunk_vals);
  void GetChunk(const std::vector<Key>& keys, std::vector<std::vector<Val>*>& chunk_vals);

};
// chunk version Add
template <typename Val>
void KVChunkClientTable<Val>::AddChunk(const std::vector<Key>& keys, const std::vector<std::vector<Val>>& chunk_vals){
  CHECK_EQ(keys.size(), chunk_vals.size());
  CHECK(!chunk_vals.empty());
  int key_size = keys.size();
  int chunk_size = chunk_vals[0].size();
  std::vector<Val> vector_chunk_vals(key_size * chunk_size);
  for (int i = 0; i < key_size; i++){
    for (int j = 0; j < chunk_size; j++){
      vector_chunk_vals[i * chunk_size + j] = chunk_vals[i][j];
    }
  }
  KVClientTable<Val>::Add(keys, vector_chunk_vals);
}

template <typename Val>
void KVChunkClientTable<Val>::GetChunk(const std::vector<Key>& keys, std::vector<std::vector<Val>*>& chunk_vals){
  KVPairs<char> kvs;
  kvs.keys = keys;
  // 1. slice
  SlicedKVs sliced = kv_table_box_.SliceChunk(kvs);
  // 2. register handle
  callback_runner_->RegisterRecvFinishHandle(kv_table_box_.app_thread_id_, kv_table_box_.model_id_,
                                             [&]() { kv_table_box_.HandleChunkFinish(third_party::SArray<Key>(keys), chunk_vals); });
  // 3. add request
  int num_reqs = sliced.size();
  callback_runner_->NewRequest(kv_table_box_.app_thread_id_, kv_table_box_.model_id_, num_reqs);
  // 4. send
  kv_table_box_.SendChunk(sliced, false);
  // 5. wait request
  callback_runner_->WaitRequest(kv_table_box_.app_thread_id_, kv_table_box_.model_id_);
}

} //namespace flexps
