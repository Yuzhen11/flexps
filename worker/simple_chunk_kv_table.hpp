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
  // The chunk version, it will tranform 2-dimension vector to 1-dimension one
  void Add(const std::vector<Key>& keys, const std::vector<std::vector<Val>>& chunk_vals);

};

// chunk version Add
template <typename Val>
void SimpleChunkKVTable::Add(const std::vector<Key>& keys, const std::vector<std:vector<Val>>& chunk_vals){
  CHECK_EQ(keys.size(), chunk_vals.size());
  CHECK(!chunk_vals.empty());
  int key_size = keys.size();
  int chunk_size = chunk_vals[0].size();
  std::vector<Val> vals(key_size * chunk_size);
  for (int i = 0; i < key_size; i++)
  {
    for (int j = 0; j < chunk_size; j++)
    {
      vals[i * chunk_size + j] = chunk_vals[i][j];
    }
  }
  SimpleKVTable<Val>::Add(keys, vals);
}

} //namespace flexps
