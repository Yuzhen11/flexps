#include "comm/channel.hpp"

#include "glog/logging.h"

namespace flexps {

Channel::Channel(uint32_t num_local_threads, uint32_t num_global_threads, std::vector<uint32_t> local_thread_ids,
                 std::unordered_map<uint32_t, uint32_t> id_map, AbstractMailbox* mailbox)
    : num_local_threads_(num_local_threads),
      num_global_threads_(num_global_threads),
      local_thread_ids_(local_thread_ids),
      id_map_(id_map),
      mailbox_(mailbox),
      m_count_(num_local_threads), m_threshold_(num_local_threads) {
  RegisterQueues();
  // Call a barrier to ensure the queues are registered
  mailbox_->Barrier();
}

Channel::~Channel() {
  DeregisterQueues();
}

void Channel::RegisterQueues() {
  local_channels_.reserve(local_thread_ids_.size());  
  for (auto tid : local_thread_ids_) {
    local_channels_.emplace_back(new LocalChannel(tid, this));
    CHECK(id_map_.find(tid) != id_map_.end());
    mailbox_->RegisterQueue(id_map_[tid], local_channels_.back()->GetQueue());
  }
}

void Channel::DeregisterQueues() {
  for (auto tid : local_thread_ids_) {
    mailbox_->DeregisterQueue(id_map_[tid]);
  }
}

std::vector<LocalChannel*> Channel::GetLocalChannels() {
  CHECK_GT(local_channels_.size(), 0);
  std::vector<LocalChannel*> ret(local_channels_.size());
  for (int i = 0; i < local_channels_.size(); ++i) {
    ret[i] = local_channels_[i].get();
  }
  return ret;
}

void Channel::PushTo(uint32_t id, const SArrayBinStream& bin) {
  CHECK_LT(id, num_global_threads_);
  uint32_t tid = id_map_[id];
  Message msg = bin.ToMsg();
  msg.meta.sender = -1;
  msg.meta.recver = tid;
  msg.meta.model_id = -1;
  msg.meta.flag = Flag::kOther;
  msg.meta.model_id = -1;
  mailbox_->Send(msg);
}

void Channel::Wait() {
  std::unique_lock<std::mutex> lk(mu_);
  uint32_t gen = m_generation_;
  if (--m_count_ == 0) {
    m_generation_ += 1;
    m_count_ = m_threshold_;
    mailbox_->Barrier();
    cv_.notify_all();
  } else {
    while (gen == m_generation_) {
      cv_.wait(lk);
    }
  }
}

}  // namespace flexps
