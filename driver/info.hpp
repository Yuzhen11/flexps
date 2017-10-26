#pragma once

#include <sstream>

#include "base/threadsafe_queue.hpp"
#include "worker/simple_range_manager.hpp"
#include "worker/abstract_callback_runner.hpp"

#include "worker/kv_client_table.hpp"
#include "worker/sparse_kv_client_table.hpp"

#include "glog/logging.h"

namespace flexps {

struct Info {
  uint32_t local_id;  // the local id for each process, {0, 1, 2...}
  uint32_t worker_id;  // the global worker id for the whole system, {0, 1, 2...}
  uint32_t thread_id;  // the global thread id

  std::string DebugString() const {
    std::stringstream ss;
    ss << "Info: {";
    ss << "local_id:" << local_id 
       << ", thread_id:" << thread_id 
       << ", worker_id:" << worker_id
       << "}";
    return ss.str();
  }

  // The wrapper function (helper) to create a KVClientTable, so that users
  // do not need to call the KVClientTable constructor with so many arguments.
  template <typename Val>
  KVClientTable<Val> CreateKVClientTable(uint32_t table_id) const;

  template <typename Val>
  SparseKVClientTable<Val> CreateSparseKVClientTable(uint32_t table_id, 
      uint32_t speculation, const std::vector<third_party::SArray<Key>>& keys) const;

  // The below fields are not supposed to be used by users
  ThreadsafeQueue<Message>* send_queue;
  std::map<uint32_t, SimpleRangeManager> range_manager_map;
  AbstractCallbackRunner* callback_runner;

};

template <typename Val>
KVClientTable<Val> Info::CreateKVClientTable(uint32_t table_id) const {
  CHECK(range_manager_map.find(table_id) != range_manager_map.end());
  KVClientTable<Val> table(thread_id, table_id, send_queue, &range_manager_map.find(table_id)->second, callback_runner);
  return table;
}
template <typename Val>
SparseKVClientTable<Val> Info::CreateSparseKVClientTable(uint32_t table_id, uint32_t speculation, const std::vector<third_party::SArray<Key>>& keys) const {
  CHECK(range_manager_map.find(table_id) != range_manager_map.end());
  SparseKVClientTable<Val> table(thread_id, table_id, send_queue, &range_manager_map.find(table_id)->second, callback_runner, speculation, keys);
  return table;
}

}  // namespace flexps
