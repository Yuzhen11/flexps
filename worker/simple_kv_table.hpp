#pragma once

#include "kv_table_box.hpp"

#include "comm/abstract_mailbox.hpp"
#include "worker/abstract_partition_manager.hpp"

namespace flexps {

/*
 * Get (optional) -> Add (optional) -> Clock ->
 *
 * Unlike KVClientTable, SimpleKVTable directly registers its recv_queue_ to mailbox
 * and thus no worker_helper_thread is needed.
 */
template <typename Val>
class SimpleKVTable {
 public:
  SimpleKVTable(uint32_t app_thread_id, uint32_t model_id, ThreadsafeQueue<Message>* const send_queue,
                const AbstractPartitionManager* const partition_manager, AbstractMailbox* const mailbox);

  ~SimpleKVTable();

  using SlicedKVs = AbstractPartitionManager::SlicedKVs;

  // The vector version
  void Add(const std::vector<Key>& keys, const std::vector<Val>& vals);
  void Get(const std::vector<Key>& keys, std::vector<Val>* vals);
  // The SArray version
  void Add(const third_party::SArray<Key>& keys, const third_party::SArray<Val>& vals);
  void Get(const third_party::SArray<Key>& keys, third_party::SArray<Val>* vals);

  void Clock();

 private:
  template <typename C>
  void Get_(const third_party::SArray<Key>& keys, C* vals);

  uint32_t current_responses = 0;
  uint32_t expected_responses = 0;
  // owned
  ThreadsafeQueue<Message> recv_queue_;
  // Not owned.
  AbstractMailbox* const mailbox_;

  // Most of the operations are delegated to KVTableBox
  KVTableBox<Val> kv_table_box_;
};

template <typename Val>
SimpleKVTable<Val>::SimpleKVTable(uint32_t app_thread_id, uint32_t model_id, ThreadsafeQueue<Message>* const send_queue,
                                  const AbstractPartitionManager* const partition_manager, AbstractMailbox* const mailbox)
    : kv_table_box_(app_thread_id, model_id, send_queue, partition_manager), mailbox_(mailbox) {
  // TODO: This is a workaround since the Engine::Run() supports KVClientTable and registers the same
  // thread id to mailbox by default for the usage of KVClientTable, and thus the id is actually
  // inside mailbox and is associated with the queue in worker_help_thread.
  mailbox_->DeregisterQueue(kv_table_box_.app_thread_id_);
  mailbox_->RegisterQueue(kv_table_box_.app_thread_id_, &recv_queue_);
}

template <typename Val>
SimpleKVTable<Val>::~SimpleKVTable() {
  mailbox_->DeregisterQueue(kv_table_box_.app_thread_id_);
}

// vector version Add
template <typename Val>
void SimpleKVTable<Val>::Add(const std::vector<Key>& keys, const std::vector<Val>& vals) {
  kv_table_box_.Add(third_party::SArray<Key>(keys), third_party::SArray<Val>(vals));
}

// SArray version Add
template <typename Val>
void SimpleKVTable<Val>::Add(const third_party::SArray<Key>& keys, const third_party::SArray<Val>& vals) {
  kv_table_box_.Add(keys, vals);
}

// vector version Get
template <typename Val>
void SimpleKVTable<Val>::Get(const std::vector<Key>& keys, std::vector<Val>* vals) {
  Get_(third_party::SArray<Key>(keys), vals);
}

// SArray version Get
template <typename Val>
void SimpleKVTable<Val>::Get(const third_party::SArray<Key>& keys, third_party::SArray<Val>* vals) {
  Get_(keys, vals);
}

// Internal version
template <typename Val>
template <typename C>
void SimpleKVTable<Val>::Get_(const third_party::SArray<Key>& keys, C* vals) {
  KVPairs<char> kvs;
  kvs.keys = keys;
  // 1. slice
  SlicedKVs sliced = kv_table_box_.Slice(kvs);
  // 2. get num requests
  expected_responses = sliced.size();
  current_responses = 0;
  // 3. send
  kv_table_box_.Send(sliced, false);
  // 4. wait request
  while (current_responses < expected_responses) {
    Message msg;
    recv_queue_.WaitAndPop(&msg);
    current_responses += 1;
    kv_table_box_.HandleMsg(msg);
    if (current_responses == expected_responses) {
      kv_table_box_.HandleFinish(keys, vals);
      current_responses = expected_responses = 0;
    }
  }
}

template <typename Val>
void SimpleKVTable<Val>::Clock() {
  kv_table_box_.Clock();
}

}  // namespace flexps
