#pragma once

#include "base/magic.hpp"
#include "base/message.hpp"
#include "base/third_party/range.h"
#include "base/third_party/sarray.h"
#include "base/threadsafe_queue.hpp"

#include "worker/abstract_callback_runner.hpp"
#include "worker/simple_range_manager.hpp"

#include "glog/logging.h"

#include <algorithm>
#include <vector>

namespace flexps {

template <typename Val>
struct KVPairs {
  third_party::SArray<Key> keys;
  third_party::SArray<Val> vals;
};

/*
 * Get/Add/Get/Add...
 */
template <typename Val>
class SparseKVClientTable {
 public:
  SparseKVClientTable(uint32_t app_thread_id, uint32_t model_id, ThreadsafeQueue<Message>* const downstream,
                const SimpleRangeManager* const range_manager, AbstractCallbackRunner* const callback_runner,
                uint32_t speculation, const std::vector<std::vector<Key>>& keys);

  void Add(const std::vector<Key>& keys, const std::vector<Val>& vals);
  void Get(std::vector<Val>* vals);

  using SlicedKVs = std::vector<std::pair<bool, KVPairs<Val>>>;

 private:
  void Clock();
  int Slice(const KVPairs<Val>& send, SlicedKVs* sliced);
  void Send(const SlicedKVs& sliced, bool is_add);
  void AddRequest(const SlicedKVs& sliced);

  void SendKeys(uint32_t version);

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
  const SimpleRangeManager* const range_manager_;

  int speculation_;
  std::vector<std::vector<Key>> keys_;
  std::vector<uint32_t> num_reqs_;
  uint32_t get_count_ = 0;
  uint32_t add_count_ = 0;
};

template <typename Val>
SparseKVClientTable<Val>::SparseKVClientTable(uint32_t app_thread_id, uint32_t model_id, ThreadsafeQueue<Message>* const downstream,
                                  const SimpleRangeManager* const range_manager,
                                  AbstractCallbackRunner* const callback_runner,
                                  uint32_t speculation, const std::vector<std::vector<Key>>& keys)
    : app_thread_id_(app_thread_id),
      model_id_(model_id),
      downstream_(downstream),
      range_manager_(range_manager),
      callback_runner_(callback_runner),
      speculation_(speculation),
      keys_(keys){
  CHECK_GE(speculation_, 0);
  CHECK_LE(speculation_, 10);
  callback_runner_->RegisterRecvHandle(app_thread_id_, model_id_, [&](Message& msg) {
    CHECK_EQ(msg.data.size(), 2);
    KVPairs<Val> kvs;
    kvs.keys = msg.data[0];
    kvs.vals = msg.data[1];
    // TODO: Need lock?
    recv_kvs_.push_back(kvs);
  });
}

template <typename Val>
void SparseKVClientTable<Val>::Add(const std::vector<Key>& keys, const std::vector<Val>& vals) {
  add_count_ += 1;
  CHECK_EQ(get_count_, add_count_);

  KVPairs<Val> kvs;
  kvs.keys = keys;
  kvs.vals = vals;
  SlicedKVs sliced;
  // 1. slice
  Slice(kvs, &sliced);
  // 2. send
  Send(sliced, true);

  // 3. clock
  Clock();  // Clock() is called here.
}

template <typename Val>
void SparseKVClientTable<Val>::Get(std::vector<Val>* vals) {
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
    if (vals->empty()) {
      vals->resize(total_val);
    } else {
      CHECK_EQ(vals->size(), total_val);
    }
    Val* p_vals = vals->data();
    for (const auto& s : recv_kvs_) {
      memcpy(p_vals, s.vals.data(), s.vals.size() * sizeof(Val));
      p_vals += s.vals.size();
    }
    recv_kvs_.clear();
  });

  // 2. send out get request for get_count_ - 1 + speculation_.
  if (get_count_ == 1) {
    // For the first Get(), Send out [0, speculation_ + 1) Get requests.
    for (int i = 0; i < speculation_ + 1; ++ i) {
      KVPairs<Val> kvs;
      kvs.keys = keys_[i];
      SlicedKVs sliced;
      int num_reqs = Slice(kvs, &sliced);
      if (i == 0) {
        // NewRequest before the first Send
        callback_runner_->NewRequest(app_thread_id_, model_id_, num_reqs);
      }
      num_reqs_.push_back(num_reqs);
      Send(sliced, false);
    }
  } else {
    // For later Get(), send out Get request in get_count_ - 1 + speculation_.
    callback_runner_->NewRequest(app_thread_id_, model_id_, num_reqs_[get_count_ - 1]);
    KVPairs<Val> kvs;
    kvs.keys = keys_[get_count_ - 1 + speculation_];
    SlicedKVs sliced;
    int num_reqs = Slice(kvs, &sliced);
    num_reqs_.push_back(num_reqs);
    Send(sliced, false);
  }
  // 3. wait request
  callback_runner_->WaitRequest(app_thread_id_, model_id_);
}

template <typename Val>
int SparseKVClientTable<Val>::Slice(const KVPairs<Val>& send, SlicedKVs* sliced) {
  CHECK_NOTNULL(range_manager_);
  sliced->resize(range_manager_->GetNumServers());
  const auto& ranges = range_manager_->GetRanges();

  size_t n = sliced->size();
  std::vector<size_t> pos(n + 1);
  const Key* begin = send.keys.begin();
  const Key* end = send.keys.end();
  for (size_t i = 0; i < n; ++i) {
    if (i == 0) {
      pos[0] = std::lower_bound(begin, end, ranges[0].begin()) - begin;
      begin += pos[0];
    } else {
      CHECK_EQ(ranges[i - 1].end(), ranges[i].begin());
    }
    size_t len = std::lower_bound(begin, end, ranges[i].end()) - begin;
    begin += len;
    pos[i + 1] = pos[i] + len;

    // don't send it to severs for empty kv
    sliced->at(i).first = (len != 0);
  }
  CHECK_EQ(pos[n], send.keys.size());

  CHECK(!send.keys.empty());


  // TODO: Do not consider lens for now
  // the length of value
  size_t k = 0, val_begin = 0, val_end = 0;
  k = send.vals.size() / send.keys.size();

  // slice
  int num_reqs = 0;
  for (size_t i = 0; i < n; ++i) {
    if (pos[i + 1] == pos[i]) {
      sliced->at(i).first = false;
      continue;
    }
    num_reqs += 1;
    sliced->at(i).first = true;
    auto& kv = sliced->at(i).second;
    kv.keys = send.keys.segment(pos[i], pos[i + 1]);
    kv.vals = send.vals.segment(pos[i] * k, pos[i + 1] * k);
  }
  return num_reqs;
}

template <typename Val>
void SparseKVClientTable<Val>::Send(const SlicedKVs& sliced, bool is_add) {
  CHECK_NOTNULL(range_manager_);
  const auto& server_thread_ids = range_manager_->GetServerThreadIds();
  CHECK_EQ(server_thread_ids.size(), sliced.size());
  for (size_t i = 0; i < sliced.size(); ++i) {
    if (!sliced[i].first) {
      continue;
    }
    Message msg;
    msg.meta.sender = app_thread_id_;
    msg.meta.recver = server_thread_ids[i];
    msg.meta.model_id = model_id_;
    msg.meta.flag = is_add ? Flag::kAdd : Flag::kGet;
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
void SparseKVClientTable<Val>::Clock() {
  CHECK_NOTNULL(range_manager_);
  const auto& server_thread_ids = range_manager_->GetServerThreadIds();
  for (uint32_t server_id : server_thread_ids) {
    Message msg;
    msg.meta.sender = app_thread_id_;
    msg.meta.recver = server_id;
    msg.meta.model_id = model_id_;
    msg.meta.flag = Flag::kClock;
    downstream_->Push(std::move(msg));
  }
}

template <typename Val>
void SparseKVClientTable<Val>::AddRequest(const SlicedKVs& sliced) {
  int num_reqs = 0;
  for (size_t i = 0; i < sliced.size(); ++i) {
    if (sliced[i].first)
      num_reqs += 1;
  }
  callback_runner_->NewRequest(app_thread_id_, model_id_, num_reqs);
}

}  // namespace flexps

