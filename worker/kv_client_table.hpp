#pragma once

#include "kv_table_box.hpp"

#include "worker/abstract_callback_runner.hpp"
#include "worker/abstract_partition_manager.hpp"

namespace flexps {

/*
 * Get (optional) -> Add (optional) -> Clock ->
 *
 * Do not need to protect HandleMsg and HandleFinish since:
 * 1. Now we assume only 1 background thread using app_blocker
 * 2. The Get() call will always wait for the result
 * If we add more background threads later, we need to lock this.
 *
 */
template <typename Val>
class KVClientTable {
 public:
  KVClientTable(uint32_t app_thread_id, uint32_t model_id, ThreadsafeQueue<Message>* const send_queue,
                const AbstractPartitionManager* const partition_manager, AbstractCallbackRunner* const callback_runner);

  // The vector version
  void Add(const std::vector<Key>& keys, const std::vector<Val>& vals);
  void Get(const std::vector<Key>& keys, std::vector<Val>* vals);
  // The SArray version
  void Add(const third_party::SArray<Key>& keys, const third_party::SArray<Val>& vals);
  void Get(const third_party::SArray<Key>& keys, third_party::SArray<Val>* vals);

  void Clock();

  using SlicedKVs = AbstractPartitionManager::SlicedKVs;

 private:
  template <typename C>
  void Get_(const third_party::SArray<Key>& keys, C* vals);

  // Not owned.
  AbstractCallbackRunner* const callback_runner_;

  // Most of the operations are delegated to KVTableBox
  KVTableBox<Val> kv_table_box_;
};

template <typename Val>
KVClientTable<Val>::KVClientTable(uint32_t app_thread_id, uint32_t model_id, ThreadsafeQueue<Message>* const send_queue,
                                  const AbstractPartitionManager* const partition_manager,
                                  AbstractCallbackRunner* const callback_runner)
    : kv_table_box_(app_thread_id, model_id, send_queue, partition_manager), callback_runner_(callback_runner) {
  callback_runner_->RegisterRecvHandle(kv_table_box_.app_thread_id_, kv_table_box_.model_id_,
                                       [&](Message& msg) { kv_table_box_.HandleMsg(msg); });
}

// vector version Add
template <typename Val>
void KVClientTable<Val>::Add(const std::vector<Key>& keys, const std::vector<Val>& vals) {
  kv_table_box_.Add(third_party::SArray<Key>(keys), third_party::SArray<Val>(vals));
}

// SArray version Add
template <typename Val>
void KVClientTable<Val>::Add(const third_party::SArray<Key>& keys, const third_party::SArray<Val>& vals) {
  kv_table_box_.Add(third_party::SArray<Key>(keys), third_party::SArray<Val>(vals));
}

// vector version Get
template <typename Val>
void KVClientTable<Val>::Get(const std::vector<Key>& keys, std::vector<Val>* vals) {
  Get_(third_party::SArray<Key>(keys), vals);
}

// SArray version Get
template <typename Val>
void KVClientTable<Val>::Get(const third_party::SArray<Key>& keys, third_party::SArray<Val>* vals) {
  Get_(keys, vals);
}

// Internal version
template <typename Val>
template <typename C>
void KVClientTable<Val>::Get_(const third_party::SArray<Key>& keys, C* vals) {
  KVPairs<char> kvs;
  kvs.keys = keys;
  // 1. slice
  SlicedKVs sliced = kv_table_box_.Slice(kvs);
  // 2. register handle
  callback_runner_->RegisterRecvFinishHandle(kv_table_box_.app_thread_id_, kv_table_box_.model_id_,
                                             [&]() { kv_table_box_.HandleFinish(keys, vals); });
  // 3. add request
  int num_reqs = sliced.size();
  callback_runner_->NewRequest(kv_table_box_.app_thread_id_, kv_table_box_.model_id_, num_reqs);
  // 4. send
  kv_table_box_.Send(sliced, false);
  // 5. wait request
  callback_runner_->WaitRequest(kv_table_box_.app_thread_id_, kv_table_box_.model_id_);
}

template <typename Val>
void KVClientTable<Val>::Clock() {
  kv_table_box_.Clock();
}

}  // namespace flexps
