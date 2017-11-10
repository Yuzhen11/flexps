#pragma once

#include "base/magic.hpp"
#include "base/message.hpp"
#include "base/third_party/range.h"
#include "base/third_party/sarray.h"
#include "base/threadsafe_queue.hpp"

#include "worker/simple_range_manager.hpp"
#include "worker/kvpairs.hpp"

#include "glog/logging.h"

#include <algorithm>
#include <vector>

namespace flexps {

/*
 * KVTableBox contains serveral operations shared by different KVTable.
 */
template <typename Val>
class KVTableBox {
 public:
  KVTableBox(uint32_t app_thread_id, uint32_t model_id, ThreadsafeQueue<Message>* const send_queue,
                const SimpleRangeManager* const range_manager);

  using SlicedKVs = std::vector<std::pair<bool, KVPairs<Val>>>;

  void Clock();
  void Slice(const KVPairs<Val>& send, SlicedKVs* sliced);
  void Send(const SlicedKVs& sliced, bool is_add);
  void Add(const third_party::SArray<Key>& keys, const third_party::SArray<Val>& vals);
  int GetNumReqs(const SlicedKVs& sliced);

  void HandleMsg(Message& msg);
  template <typename C>
  void HandleFinish(KVPairs<Val>& kvs, const third_party::SArray<Key>& keys, C* vals);

  uint32_t app_thread_id_;
  uint32_t model_id_;
 private:
  // Not owned.
  ThreadsafeQueue<Message>* const send_queue_;
  // Not owned.
  const SimpleRangeManager* const range_manager_;

  std::vector<KVPairs<Val>> recv_kvs_;
};

template <typename Val>
KVTableBox<Val>::KVTableBox(uint32_t app_thread_id, uint32_t model_id, ThreadsafeQueue<Message>* const send_queue,
                                  const SimpleRangeManager* const range_manager)
    : app_thread_id_(app_thread_id),
      model_id_(model_id),
      send_queue_(send_queue),
      range_manager_(range_manager) {
}

// SArray version Add
template <typename Val>
void KVTableBox<Val>::Add(const third_party::SArray<Key>& keys, const third_party::SArray<Val>& vals) {
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
void KVTableBox<Val>::Slice(const KVPairs<Val>& send, SlicedKVs* sliced) {
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
void KVTableBox<Val>::Send(const SlicedKVs& sliced, bool is_add) {
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
    send_queue_->Push(std::move(msg));
  }
}

template <typename Val>
void KVTableBox<Val>::Clock() {
  CHECK_NOTNULL(range_manager_);
  const auto& server_thread_ids = range_manager_->GetServerThreadIds();
  for (uint32_t server_id : server_thread_ids) {
    Message msg;
    msg.meta.sender = app_thread_id_;
    msg.meta.recver = server_id;
    msg.meta.model_id = model_id_;
    msg.meta.flag = Flag::kClock;
    send_queue_->Push(std::move(msg));
  }
}

template <typename Val>
int KVTableBox<Val>::GetNumReqs(const SlicedKVs& sliced) {
  int num_reqs = 0;
  for (size_t i = 0; i < sliced.size(); ++i) {
    if (sliced[i].first)
      num_reqs += 1;
  }
  return num_reqs;
}

template <typename Val>
void KVTableBox<Val>::HandleMsg(Message& msg) {
  CHECK_EQ(msg.data.size(), 2);
  KVPairs<Val> kvs;
  kvs.keys = msg.data[0];
  kvs.vals = msg.data[1];
  recv_kvs_.push_back(kvs);
}


template <typename Val>
template <typename C>
void KVTableBox<Val>::HandleFinish(KVPairs<Val>& kvs, const third_party::SArray<Key>& keys, C* vals) {
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
  vals->resize(total_val);
  Val* p_vals = vals->data();
  for (const auto& s : recv_kvs_) {
    memcpy(p_vals, s.vals.data(), s.vals.size() * sizeof(Val));
    p_vals += s.vals.size();
  }
  recv_kvs_.clear();
}

}  // namespace flexps
