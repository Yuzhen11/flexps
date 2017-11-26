#pragma once

#include "kv_table_box.hpp"
#include "simple_kv_table.hpp"

#include "comm/abstract_mailbox.hpp"
#include "worker/abstract_partition_manager.hpp"

namespace flexps {

/*
 * Get (optional) -> Add (optional) -> Clock ->
 *
 * SimpleChunkKVTable is chunk-based version of simpleKVTable
 * 
 */
template <typename Val>
class SimpleChunkKVTable: public SimpleKVTable<Val> {
 public:
  using SimpleKVTable<Val>::SimpleKVTable;
  // The chunk version, it will tranform 2-dimension vector to 1-dimension one
  void AddChunk(const std::vector<Key>& keys, const std::vector<std::vector<Val>>& chunk_vals);
  void GetChunk(const std::vector<Key>& keys, std::vector<std::vector<Val>*>& chunk_vals);
};

// chunk version Add
template <typename Val>
void SimpleChunkKVTable<Val>::AddChunk(const std::vector<Key>& keys, const std::vector<std::vector<Val>>& chunk_vals){
  CHECK_EQ(keys.size(), chunk_vals.size());
  CHECK(!chunk_vals.empty());
  int key_size = keys.size();
  int chunk_size = chunk_vals[0].size();
  std::vector<Key> chunk_keys(key_size * chunk_size);
  std::vector<Val> vector_chunk_vals(key_size * chunk_size);
  if(chunk_size == 1){
    for (int i = 0; i < key_size; i++){
      vector_chunk_vals[i] = chunk_vals[i][0];
      chunk_keys[i] = keys[i];
    }
  }
  else{
    for (int i = 0; i < key_size; i++){
      for (int j = 0; j < chunk_size; j++){
        vector_chunk_vals[i * chunk_size + j] = chunk_vals[i][j];
        chunk_keys[i * chunk_size + j] =  keys[i]  * chunk_size + j;
      }
    }
  }
  SimpleKVTable<Val>::Add(chunk_keys, vector_chunk_vals);
}

template <typename Val>
void SimpleChunkKVTable<Val>::GetChunk(const std::vector<Key>& keys, std::vector<std::vector<Val>*>& chunk_vals){
  CHECK_EQ(keys.size(), chunk_vals.size());
  int key_size = keys.size();
  int chunk_size = chunk_vals[0]->size();
  std::vector<Val> ret_vals;
  std::vector<Key> chunk_keys;
  for(int i = 0; i < key_size; i++){
    for(int j = 0; j < chunk_size; j++){
      chunk_keys.push_back(keys[i]  * chunk_size + j);
    }
  }
  SimpleKVTable<Val>::Get(chunk_keys, &ret_vals);
  for(int i = 0; i < key_size; i++){
    for(int j = 0; j < chunk_size; j++){
      (*chunk_vals[i])[j] = ret_vals[i * chunk_size + j];
    }
  }
}
} //namespace flexps
