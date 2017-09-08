#pragma once

#include "base/magic.hpp"
#include "base/message.hpp"
#include "base/threadsafe_queue.hpp"
#include "base/third_party/range.h"
#include "base/third_party/sarray.h"

#include "abstract_range_manager.hpp"

namespace flexps {

template <typename Val>
struct KVPairs {
    third_party::SArray<Key> keys;
    third_party::SArray<Val> vals;
};

template<typename Val>
class KVClientTable {
 public:
  KVClientTable(uint32_t app_thread_id, uint32_t model_id, 
      ThreadsafeQueue<Message>* const downstream,
      const AbstractRangeManager* const range_manager)
    : app_thread_id_(app_thread_id), model_id_(model_id),
    downstream_(downstream), range_manager_(range_manager) {}

  void Init();

  void Add(const std::vector<Key>& keys, const std::vector<Val>& vals);
  void Get(const std::vector<Key>& keys, std::vector<Val>* vals);
  void Clock();

  using SlicedKVs = std::vector<std::pair<bool, KVPairs<Val>>>;
 private:
  void Slice(const KVPairs<Val>& send, SlicedKVs* sliced);
  void Send(const SlicedKVs& sliced);
  void AddRequest(const SlicedKVs& sliced);

  uint32_t app_thread_id_;
  uint32_t model_id_;

  // Not owned.
  ThreadsafeQueue<Message>* const downstream_;
  // Not owned.
  // AbstractCallbackRunner* const callback_runner_;
  // Not owned.
  const AbstractRangeManager* const range_manager_; 
};

template<typename Val>
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
  Send(sliced);
}

template<typename Val>
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
      CHECK_EQ(ranges[i-1].end(), ranges[i].begin());
    }
    size_t len = std::lower_bound(begin, end, ranges[i].end()) - begin;
    begin += len;
    pos[i+1] = pos[i] + len;

    // don't send it to severs for empty kv
    sliced->at(i).first = (len != 0);
  }
  CHECK_EQ(pos[n], send.keys.size());
  if (send.keys.empty()) return;

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

template<typename Val>
void KVClientTable<Val>::Send(const SlicedKVs& sliced) {
  for (size_t i = 0; i < sliced.size(); ++ i) {
    if (!sliced[i].first) {
      continue;
    }
    Message msg;
    // TODO: Fill the meta
    // msg.meta = ...;
    const auto& kvs = sliced[i].second;
    if (kvs.keys.size()) {
      msg.AddData(kvs.keys);
      msg.AddData(kvs.vals);
    }
    downstream_->Push(std::move(msg));
  }
}

template<typename Val>
void KVClientTable<Val>::AddRequest(const SlicedKVs& sliced) {
}

}  // namespace flexps
