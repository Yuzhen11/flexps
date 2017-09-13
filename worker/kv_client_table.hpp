#pragma once

#include "base/magic.hpp"
#include "base/message.hpp"
#include "base/third_party/range.h"
#include "base/third_party/sarray.h"
#include "base/threadsafe_queue.hpp"

#include "abstract_callback_runner.hpp"
#include "abstract_range_manager.hpp"

#include "glog/logging.h"

#include <algorithm>
#include <vector>

namespace flexps {

template <typename Val>
struct KVPairs {
  third_party::SArray<Key> keys;
  third_party::SArray<Val> vals;
};

template <typename Val>
class KVClientTable {
 public:
  KVClientTable(uint32_t app_thread_id, uint32_t model_id, ThreadsafeQueue<Message>* const downstream,
                const AbstractRangeManager* const range_manager, AbstractCallbackRunner* const callback_runner);

  void Init();

  void Add(const std::vector<Key>& keys, const std::vector<Val>& vals);
  void Get(const std::vector<Key>& keys, std::vector<Val>* vals);
  void Clock();

  using SlicedKVs = std::vector<std::pair<bool, KVPairs<Val>>>;

 private:
  void Slice(const KVPairs<Val>& send, SlicedKVs* sliced);
  void Send(const SlicedKVs& sliced, bool is_add);
  void AddRequest(const SlicedKVs& sliced);

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
  const AbstractRangeManager* const range_manager_;
};

template <typename Val>
KVClientTable<Val>::KVClientTable(uint32_t app_thread_id, uint32_t model_id, ThreadsafeQueue<Message>* const downstream,
                                  const AbstractRangeManager* const range_manager,
                                  AbstractCallbackRunner* const callback_runner)
    : app_thread_id_(app_thread_id),
      model_id_(model_id),
      downstream_(downstream),
      range_manager_(range_manager),
      callback_runner_(callback_runner) {
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
void KVClientTable<Val>::Add(const std::vector<Key>& keys, const std::vector<Val>& vals) {
  // third_party::SArray<Key> s_keys(keys);
  // third_party::SArray<Val> s_vals(vals);
  KVPairs<Val> kvs;
  kvs.keys = keys;
  kvs.vals = vals;
  SlicedKVs sliced;
  // 1. slice
  Slice(kvs, &sliced);
  // 2. send
  Send(sliced, true);
}

template <typename Val>
void KVClientTable<Val>::Get(const std::vector<Key>& keys, std::vector<Val>* vals) {
  KVPairs<Val> kvs;
  kvs.keys = keys;
  SlicedKVs sliced;
  // 1. slice
  Slice(kvs, &sliced);
  // 2. register handle
  callback_runner_->RegisterRecvFinishHandle(app_thread_id_, model_id_, [&]() {
    size_t total_key = 0, total_val = 0;
    for (const auto& s : recv_kvs_) {
      third_party::Range range = third_party::FindRange(kvs.keys, s.keys.front(), s.keys.back() + 1);
      CHECK_EQ(range.size(), s.keys.size()) << "unmatched keys size from one server";
      total_key += s.keys.size();
      total_val += s.vals.size();
    }
    CHECK_EQ(total_key, keys.size()) << "lost some servers?";
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
  // 3. add request
  AddRequest(sliced);
  // 4. send
  Send(sliced, false);
  // 5. wait request
  callback_runner_->WaitRequest(app_thread_id_, model_id_);
}

template <typename Val>
void KVClientTable<Val>::Slice(const KVPairs<Val>& send, SlicedKVs* sliced) {
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
  if (send.keys.empty())
    return;

  // TODO: Do not consider lens for now
  // the length of value
  size_t k = 0, val_begin = 0, val_end = 0;
  k = send.vals.size() / send.keys.size();

  // slice
  for (size_t i = 0; i < n; ++i) {
    if (pos[i + 1] == pos[i]) {
      sliced->at(i).first = false;
      continue;
    }
    sliced->at(i).first = true;
    auto& kv = sliced->at(i).second;
    kv.keys = send.keys.segment(pos[i], pos[i + 1]);
    kv.vals = send.vals.segment(pos[i] * k, pos[i + 1] * k);
  }
}

template <typename Val>
void KVClientTable<Val>::Send(const SlicedKVs& sliced, bool is_add) {
  for (size_t i = 0; i < sliced.size(); ++i) {
    if (!sliced[i].first) {
      continue;
    }
    Message msg;
    // TODO: Fill the meta
    // msg.meta = ...;
    msg.meta.sender = app_thread_id_;
    msg.meta.recver = i;
    msg.meta.model_id = model_id_;
    msg.meta.flag = is_add ? Flag::kAdd : Flag::kGet;
    const auto& kvs = sliced[i].second;
    if (kvs.keys.size()) {
      msg.AddData(kvs.keys);
      msg.AddData(kvs.vals);
    }
    downstream_->Push(std::move(msg));
  }
}

template <typename Val>
void KVClientTable<Val>::AddRequest(const SlicedKVs& sliced) {
  int num_reqs = 0;
  for (size_t i = 0; i < sliced.size(); ++i) {
    if (sliced[i].first)
      num_reqs += 1;
  }
  callback_runner_->NewRequest(app_thread_id_, model_id_, num_reqs);
}

}  // namespace flexps
