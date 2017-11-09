#pragma once

#include "base/magic.hpp"
#include "base/message.hpp"
#include "base/third_party/range.h"
#include "base/third_party/sarray.h"
#include "base/threadsafe_queue.hpp"

#include "comm/abstract_mailbox.hpp"
#include "worker/simple_range_manager.hpp"
#include "worker/kvpairs.hpp"

#include "glog/logging.h"

#include <algorithm>
#include <vector>

namespace flexps {

/*
 * Get (optional) -> Add (optional) -> Clock ->
 *
 * Unlike KVClientTable, KVTable directly registers its recv_queue_ to mailbox
 * and thus no worker_helper_thread is needed.
 */
template <typename Val>
class KVTable {
 public:
  KVTable(uint32_t app_thread_id, uint32_t model_id, ThreadsafeQueue<Message>* const send_queue,
                const SimpleRangeManager* const range_manager, AbstractMailbox* const mailbox);

  ~KVTable();
  // The vector version
  void Add(const std::vector<Key>& keys, const std::vector<Val>& vals);
  void Get(const std::vector<Key>& keys, std::vector<Val>* vals);
  // The SArray version
  void Add(const third_party::SArray<Key>& keys, const third_party::SArray<Val>& vals);
  void Get(const third_party::SArray<Key>& keys, third_party::SArray<Val>* vals);

  void Clock();

  using SlicedKVs = std::vector<std::pair<bool, KVPairs<Val>>>;

 private:
  template <typename C>
  void Get_(const third_party::SArray<Key>& keys, C* vals);

  void Slice_(const KVPairs<Val>& send, SlicedKVs* sliced);
  void Send_(const SlicedKVs& sliced, bool is_add);
  void AddRequest_(const SlicedKVs& sliced);
  void HandleMsg_(Message& msg);
  template <typename C>
  void HandleFinish_(KVPairs<Val>& kvs, const third_party::SArray<Key>& keys, C* vals);

  uint32_t app_thread_id_;
  uint32_t model_id_;

  std::vector<KVPairs<Val>> recv_kvs_;
  uint32_t current_responses = 0;
  uint32_t expected_responses = 0;
  // Not owned.
  ThreadsafeQueue<Message>* const send_queue_;
  // owned
  ThreadsafeQueue<Message> recv_queue_;
  // Not owned.
  const SimpleRangeManager* const range_manager_;
  // Not owned.
  AbstractMailbox* const mailbox_;
};

template <typename Val>
KVTable<Val>::KVTable(uint32_t app_thread_id, uint32_t model_id, ThreadsafeQueue<Message>* const send_queue,
                                  const SimpleRangeManager* const range_manager,
                                  AbstractMailbox* const mailbox)
    : app_thread_id_(app_thread_id),
      model_id_(model_id),
      send_queue_(send_queue),
      range_manager_(range_manager),
      mailbox_(mailbox) {
  // TODO: This is a workaround since the Engine::Run() supports KVClientTable and registers the same
  // thread id to mailbox by default for the usage of KVClientTable, and thus the id is actually
  // inside mailbox and is associated with the queue in worker_help_thread.
  mailbox_->DeregisterQueue(app_thread_id_);
  mailbox_->RegisterQueue(app_thread_id_, &recv_queue_);
}

// vector version Add
template <typename Val>
void KVTable<Val>::Add(const std::vector<Key>& keys, const std::vector<Val>& vals) {
  Add(third_party::SArray<Key>(keys), third_party::SArray<Val>(vals));
}

template <typename Val>
KVTable<Val>::~KVTable(){
  mailbox_->DeregisterQueue(app_thread_id_);
}

// SArray version Add
template <typename Val>
void KVTable<Val>::Add(const third_party::SArray<Key>& keys, const third_party::SArray<Val>& vals) {
  KVPairs<Val> kvs;
  kvs.keys = keys;
  kvs.vals = vals;
  SlicedKVs sliced;
  // 1. slice
  Slice_(kvs, &sliced);
  // 2. send
  Send_(sliced, true);
}


// vector version Get 
template <typename Val>
void KVTable<Val>::Get(const std::vector<Key>& keys, std::vector<Val>* vals) {
  Get_(third_party::SArray<Key>(keys), vals);
}

// SArray version Get
template <typename Val>
void KVTable<Val>::Get(const third_party::SArray<Key>& keys, third_party::SArray<Val>* vals) {
  Get_(keys, vals);
}

// Internal version
template <typename Val>
template <typename C>
void KVTable<Val>::Get_(const third_party::SArray<Key>& keys, C* vals) {
  KVPairs<Val> kvs;
  kvs.keys = keys;
  SlicedKVs sliced;
  // 1. slice
  Slice_(kvs, &sliced);
  // 2. add request
  AddRequest_(sliced);
  // 3. send
  Send_(sliced, false);
  // 4. wait request
  while (current_responses < expected_responses) {
    Message msg;
    recv_queue_.WaitAndPop(&msg);
    current_responses += 1;
    HandleMsg_(msg);
    if (current_responses == expected_responses) {
      HandleFinish_(kvs, keys, vals);
      current_responses = expected_responses = 0;
    }
  }
}

template <typename Val>
void KVTable<Val>::Slice_(const KVPairs<Val>& send, SlicedKVs* sliced) {
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
void KVTable<Val>::Send_(const SlicedKVs& sliced, bool is_add) {
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
void KVTable<Val>::Clock() {
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
void KVTable<Val>::AddRequest_(const SlicedKVs& sliced) {
  int num_reqs = 0;
  for (size_t i = 0; i < sliced.size(); ++i) {
    if (sliced[i].first)
      num_reqs += 1;
  }
  current_responses = 0;
  expected_responses = num_reqs;
}

template <typename Val>
void KVTable<Val>::HandleMsg_(Message& msg)
{
  CHECK_EQ(msg.data.size(), 2);
  KVPairs<Val> kvs;
  kvs.keys = msg.data[0];
  kvs.vals = msg.data[1];
  recv_kvs_.push_back(kvs);
}


template <typename Val>
template <typename C>
void KVTable<Val>::HandleFinish_(KVPairs<Val>& kvs, const third_party::SArray<Key>& keys, C* vals)
{
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
