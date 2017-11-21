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
  // The chunk version, it will tranform 2-dimension vector to 1-dimension one
  void Add(const std::vector<Key>& keys, const std::vector<std::vector<Val>>& chunk_vals);

};
// chunk version Add
template <typename Val>
void KVChunkClientTable<Val>::Add(const std::vector<Key>& keys, const std::vector<std::vector<Val>>& chunk_vals){
  CHECK_EQ(keys.size(), chunk_vals.size());
  CHECK(!chunk_vals.empty());
  int key_size = keys.size();
  int chunk_size = chunk_vals[0].size();
  std::vector<Val> vals(key_size * chunk_size);
  if(chunk_size == 1){
    for (int i = 0; i < key_size; i++)
        vals[i] = chunk_vals[i][0];
  }
  else{
    for (int i = 0; i < key_size; i++)
      for (int j = 0; j < chunk_size; j++)
        vals[i * chunk_size + j] = chunk_vals[i][j];
  }
  KVClientTable<Val>::Add(keys, vals);
}


} //namespace flexps

