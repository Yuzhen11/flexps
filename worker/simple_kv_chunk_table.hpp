#pragma once

#include "simple_kv_table.hpp"
#include "kv_table_box.hpp"

#include "worker/abstract_callback_runner.hpp"
#include "worker/abstract_partition_manager.hpp"

namespace flexps {
/*
 * Get (optional) -> Add (optional) -> Clock ->
 *
 * It is chunk-based version of SimpleKVTable
 *
 */                                                                                                                 

template <typename Val>
class SimpleKVChunkTable : public SimpleKVTable<Val> {
 public:
  SimpleKVChunkTable(const SimpleKVChunkTable&) = delete;
  SimpleKVChunkTable& operator=(const SimpleKVChunkTable&) = delete;
  SimpleKVChunkTable(SimpleKVChunkTable&& other) = delete;
  SimpleKVChunkTable& operator=(SimpleKVChunkTable&& other) = delete;

  using SimpleKVTable<Val>::SimpleKVTable;
  using SimpleKVTable<Val>::kv_table_box_;
  using SimpleKVTable<Val>::recv_queue_;
  using SimpleKVTable<Val>::mailbox_;
  using SlicedKVs = AbstractPartitionManager::SlicedKVs;
  // The chunk version, it will tranform 2-dimension vector to 1-dimension one
  void AddChunk(const std::vector<Key>& keys, const std::vector<std::vector<Val>>& chunk_vals);
  void GetChunk(const std::vector<Key>& keys, std::vector<std::vector<Val>*>& chunk_vals);

 private:
  uint32_t current_responses = 0;
  uint32_t expected_responses = 0;
};
// chunk version Add
template <typename Val>
void SimpleKVChunkTable<Val>::AddChunk(const std::vector<Key>& keys, const std::vector<std::vector<Val>>& chunk_vals){
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
  SimpleKVTable<Val>::kv_table_box_.AddChunk(third_party::SArray<Key>(keys), third_party::SArray<Val>(vector_chunk_vals));
}

template <typename Val>
void SimpleKVChunkTable<Val>::GetChunk(const std::vector<Key>& keys, std::vector<std::vector<Val>*>& chunk_vals){
  KVPairs<char> kvs;
  kvs.keys = keys;
  // 1. slice
  SlicedKVs sliced = kv_table_box_.SliceChunk(kvs);
  // 2. get num requests
  expected_responses = sliced.size();
  current_responses = 0;
  // 3. send
  kv_table_box_.SendChunk(sliced, false);
  // 4. wait request
  while (current_responses < expected_responses) {
    Message msg;
    recv_queue_.WaitAndPop(&msg);
    current_responses += 1;
    kv_table_box_.HandleMsg(msg);
    if (current_responses == expected_responses) {
      kv_table_box_.HandleChunkFinish(third_party::SArray<Key>(keys), chunk_vals);
      current_responses = expected_responses = 0;
    }
  }
}

} //namespace flexps
