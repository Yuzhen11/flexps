#pragma once

#include "base/magic.hpp"
#include "base/message.hpp"
#include "base/third_party/range.h"
#include "base/third_party/sarray.h"
#include "base/threadsafe_queue.hpp"

#include "worker/abstract_callback_runner.hpp"
#include "worker/abstract_partition_manager.hpp"
#include "worker/kvpairs.hpp"
#include "worker/simple_range_manager.hpp"

#include "glog/logging.h"

#include <algorithm>
#include <vector>

namespace flexps {

/*
 * No need to call Clock explicitly.
 *
 * Get (requried) -> Add (optional) ->
 *
 * Get -> Add ->
 * Get(Clock) -> Add ->
 * Get(Clock) -> Add ->
 * ...
 */
template <typename Val>
class SparseKVClientTable {
 public:
  SparseKVClientTable(uint32_t app_thread_id, uint32_t model_id, ThreadsafeQueue<Message>* const downstream,
                      const AbstractPartitionManager* const partition_manager,
                      AbstractCallbackRunner* const callback_runner, uint32_t speculation,
                      const std::vector<third_party::SArray<Key>>& keys);

  SparseKVClientTable(uint32_t app_thread_id, uint32_t model_id, ThreadsafeQueue<Message>* const downstream,
                      const AbstractPartitionManager* const partition_manager,
                      AbstractCallbackRunner* const callback_runner, uint32_t speculation,
                      const std::vector<std::vector<Key>>& keys);
  SparseKVClientTable(const SparseKVClientTable&) = delete;
  SparseKVClientTable& operator=(const SparseKVClientTable&) = delete;
  SparseKVClientTable(SparseKVClientTable&& other) = delete;
  SparseKVClientTable& operator=(SparseKVClientTable&& other) = delete;

  // The vector version
  void Add(const std::vector<Key>& keys, const std::vector<Val>& vals);
  void Get(std::vector<Val>* vals);
  // The SArray version
  void Add(const third_party::SArray<Key>& keys, const third_party::SArray<Val>& vals);
  void Get(third_party::SArray<Val>* vals);

  using SlicedKVs = AbstractPartitionManager::SlicedKVs;

 private:
  template <typename C>
  void Get_(C* vals);

  void Clock_();
  void Send_(const SlicedKVs& sliced, bool is_add, int version);
  void AddRequest_(const SlicedKVs& sliced);
  void Setup_();

  uint32_t app_thread_id_;
  uint32_t model_id_;

  // Do not need to protect this variable since:
  // 1. Now we assume only 1 background thread using app_blocker
  // 2. The Get() call will always wait for the result
  // If we add more background threads later, we need to lock this.
  std::vector<KVPairs<Val>> recv_kvs_;

  // Not owned.
  ThreadsafeQueue<Message>* const downstream_;
  // Not owned.
  AbstractCallbackRunner* const callback_runner_;
  // Not owned.
  const AbstractPartitionManager* const partition_manager_;

  int speculation_;
  std::vector<third_party::SArray<Key>> keys_;
  std::vector<uint32_t> num_reqs_;
  uint32_t get_count_ = 0;
  uint32_t add_count_ = 0;
};

template <typename Val>
SparseKVClientTable<Val>::SparseKVClientTable(uint32_t app_thread_id, uint32_t model_id,
                                              ThreadsafeQueue<Message>* const downstream,
                                              const AbstractPartitionManager* const partition_manager,
                                              AbstractCallbackRunner* const callback_runner, uint32_t speculation,
                                              const std::vector<std::vector<Key>>& keys)
    : app_thread_id_(app_thread_id),
      model_id_(model_id),
      downstream_(downstream),
      partition_manager_(partition_manager),
      callback_runner_(callback_runner),
      speculation_(speculation) {
  for (auto& key : keys) {
    keys_.push_back(third_party::SArray<Key>(key));
  }
  Setup_();
}

template <typename Val>
SparseKVClientTable<Val>::SparseKVClientTable(uint32_t app_thread_id, uint32_t model_id,
                                              ThreadsafeQueue<Message>* const downstream,
                                              const AbstractPartitionManager* const partition_manager,
                                              AbstractCallbackRunner* const callback_runner, uint32_t speculation,
                                              const std::vector<third_party::SArray<Key>>& keys)
    : app_thread_id_(app_thread_id),
      model_id_(model_id),
      downstream_(downstream),
      partition_manager_(partition_manager),
      callback_runner_(callback_runner),
      speculation_(speculation),
      keys_(keys) {
  Setup_();
}

template <typename Val>
void SparseKVClientTable<Val>::Setup_() {
  CHECK_GE(speculation_, 0);
  CHECK_LE(speculation_, 50);
  callback_runner_->RegisterRecvHandle(app_thread_id_, model_id_, [&](Message& msg) {
    CHECK_EQ(msg.data.size(), 2);
    KVPairs<Val> kvs;
    kvs.keys = msg.data[0];
    kvs.vals = msg.data[1];
    // TODO: Need lock?
    recv_kvs_.push_back(kvs);
  });
}

// vector version Add
template <typename Val>
void SparseKVClientTable<Val>::Add(const std::vector<Key>& keys, const std::vector<Val>& vals) {
  Add(third_party::SArray<Key>(keys), third_party::SArray<Val>(vals));
}

// SArray version Add
template <typename Val>
void SparseKVClientTable<Val>::Add(const third_party::SArray<Key>& keys, const third_party::SArray<Val>& vals) {
  add_count_ += 1;
  CHECK_EQ(get_count_, add_count_);

  KVPairs<char> kvs;
  kvs.keys = keys;
  kvs.vals = vals;
  // 1. slice
  CHECK_NOTNULL(partition_manager_);
  SlicedKVs sliced = partition_manager_->Slice(kvs);
  // 2. send
  Send_(sliced, true, get_count_ - 1);
}

// vector version Get
template <typename Val>
void SparseKVClientTable<Val>::Get(std::vector<Val>* vals) {
  Get_(vals);
}

// SArray version Get
template <typename Val>
void SparseKVClientTable<Val>::Get(third_party::SArray<Val>* vals) {
  Get_(vals);
}

// Internal version
template <typename Val>
template <typename C>
void SparseKVClientTable<Val>::Get_(C* vals) {
  CHECK_EQ(get_count_, add_count_);
  get_count_ += 1;

  // 1. register handle
  callback_runner_->RegisterRecvFinishHandle(app_thread_id_, model_id_, [&]() {
    size_t total_key = 0, total_val = 0;
    for (const auto& s : recv_kvs_) {
      total_key += s.keys.size();
      total_val += s.vals.size();
    }
    CHECK_EQ(total_key, keys_[get_count_ - 1].size()) << "lost some servers?";
    std::sort(recv_kvs_.begin(), recv_kvs_.end(),
              [](const KVPairs<Val>& a, const KVPairs<Val>& b) { return a.keys.front() < b.keys.front(); });
    CHECK_NOTNULL(vals);
    vals->resize(total_val);
    Val* p_vals = vals->data();
    for (const auto& s : recv_kvs_) {
      memcpy(p_vals, s.vals.data(), s.vals.size() * sizeof(Val));
      p_vals += s.vals.size();
    }
    recv_kvs_.clear();
  });

  // Actually the communication pattern required by the server side is to send
  // the speculation_ number of Get requests. And before each Clock, a new Get
  // request should be sent.
  // For here, we send out speculation_ + 1 Get requesets in the first iteration
  // and send out each Clock before the next Get.
  //
  // For example, for speculation_ = 1
  // Required:
  // Get_0    Get_1 Clock_0     Get_2 Clock_1
  // Ours:
  // Get_0 Get_1      Clock_0 Get_2      Clock_1 Get_3
  //
  // 2. send out get request for get_count_ - 1 + speculation_.
  if (get_count_ == 1) {
    // For the first Get(), Send out [0, speculation_ + 1) Get requests.
    for (int i = 0; i < speculation_ + 1; ++i) {
      KVPairs<char> kvs;
      CHECK_LT(i, keys_.size());
      kvs.keys = keys_[i];
      CHECK_NOTNULL(partition_manager_);
      SlicedKVs sliced = partition_manager_->Slice(kvs);
      int num_reqs = sliced.size();
      if (i == 0) {
        // NewRequest before the first Send
        callback_runner_->NewRequest(app_thread_id_, model_id_, num_reqs);
      }
      num_reqs_.push_back(num_reqs);
      Send_(sliced, false, i);
    }
  } else {
    // For later Get(), send out Get request in get_count_ - 1 + speculation_.
    callback_runner_->NewRequest(app_thread_id_, model_id_, num_reqs_[get_count_ - 1]);
    // Call Clock after NewRequest
    Clock_();  // Clock_() is called here. When the clock is called. The response message may be sent back.
    KVPairs<char> kvs;
    CHECK_LT(get_count_ - 1 + speculation_, keys_.size());
    kvs.keys = keys_[get_count_ - 1 + speculation_];
    CHECK_NOTNULL(partition_manager_);
    SlicedKVs sliced = partition_manager_->Slice(kvs);
    int num_reqs = sliced.size();
    num_reqs_.push_back(num_reqs);
    Send_(sliced, false, get_count_ - 1 + speculation_);
  }
  // 3. wait request
  callback_runner_->WaitRequest(app_thread_id_, model_id_);
}

template <typename Val>
void SparseKVClientTable<Val>::Send_(const SlicedKVs& sliced, bool is_add, int version) {
  CHECK_NOTNULL(partition_manager_);
  for (size_t i = 0; i < sliced.size(); ++i) {
    Message msg;
    msg.meta.sender = app_thread_id_;
    msg.meta.recver = sliced[i].first;
    msg.meta.model_id = model_id_;
    msg.meta.flag = is_add ? Flag::kAdd : Flag::kGet;
    msg.meta.version = version;
    const auto& kvs = sliced[i].second;
    if (kvs.keys.size()) {
      msg.AddData(kvs.keys);
      if (is_add) {
        msg.AddData(kvs.vals);
      }
    }
    downstream_->Push(std::move(msg));
  }
}

template <typename Val>
void SparseKVClientTable<Val>::Clock_() {
  CHECK_NOTNULL(partition_manager_);
  const auto& server_thread_ids = partition_manager_->GetServerThreadIds();
  for (uint32_t server_id : server_thread_ids) {
    Message msg;
    msg.meta.sender = app_thread_id_;
    msg.meta.recver = server_id;
    msg.meta.model_id = model_id_;
    msg.meta.flag = Flag::kClock;
    msg.meta.version = get_count_ - 2;  // -2 because it is called in Get for the next iter.
    downstream_->Push(std::move(msg));
  }
}

template <typename Val>
void SparseKVClientTable<Val>::AddRequest_(const SlicedKVs& sliced) {
  callback_runner_->NewRequest(app_thread_id_, model_id_, sliced.size());
}

}  // namespace flexps
