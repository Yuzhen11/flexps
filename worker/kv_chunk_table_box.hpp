#pragma once

#include "base/magic.hpp"
#include "base/message.hpp"
#include "base/third_party/range.h"
#include "base/third_party/sarray.h"
#include "base/threadsafe_queue.hpp"

#include "worker/kvpairs.hpp"
#include "worker/simple_range_manager.hpp"

#include "glog/logging.h"

#include <algorithm>
#include <vector>

namespace flexps {

/*
 * KVTableBox contains serveral operations shared by different KVTable.
 */
template <typename Val>
class KVChunkTableBox: public KVTableBox {
 public:
  void Send(const SlicedKVs& sliced, bool is_add);
  SlicedKVs Slice(const KVPairs<char>& send);

  void HandleMsg(Message& msg);
  template <typename C>
  void HandleFinish(const third_party::SArray<Key>& keys, C* vals);

};


template <typename Val>
void KVTableBox<Val>::Send(const SlicedKVs& sliced, bool is_add) {
  CHECK_NOTNULL(partition_manager_);
  for (size_t i = 0; i < sliced.size(); ++i) {
    Message msg;
    msg.meta.sender = app_thread_id_;
    msg.meta.recver = sliced[i].first;
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
template <typename C>
void KVTableBox<Val>::HandleFinish(const third_party::SArray<Key>& keys, C* vals) {
  size_t total_key = 0, total_val = 0;
  for (const auto& s : recv_kvs_) {
    third_party::Range range = third_party::FindRange(keys, s.keys.front(), s.keys.back() + 1);
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
