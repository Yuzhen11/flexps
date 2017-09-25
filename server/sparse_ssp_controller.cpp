#include "server/sparse_ssp_controller.hpp"

#include "glog/logging.h"

#include <iostream>
#include <sstream>

namespace flexps {

std::list<Message> SparseSSPController::Clock(int progress, int sender, int updated_min_clock, int min_clock) {
  if (future_keys_[sender].size() > 0 && future_keys_[sender].front().first == progress - 1) {
    RemoveRecord(progress - 1, sender, future_keys_[sender].front().second);
    future_keys_[sender].pop();
  }

  std::list<Message> rets;
  // Handle too_fast_buffer_ if min_clock updated
  // Maybe it's clear to move this logic to handle updated_min_clock out.
  if (updated_min_clock != -1) {
    for (auto& msg : too_fast_buffer_) {
      int forwarded_worker_id = -1;
      int forwarded_version = -1;
      if (HasConflict(third_party::SArray<uint32_t>(msg.data[0]), min_clock,
            msg.meta.version - staleness_ - 1, forwarded_worker_id, forwarded_version)) {
        CHECK(forwarded_version >= min_clock) << "[Error]SparseSSPModel: forwarded_version invalid";
        Push(forwarded_version, msg, forwarded_worker_id);
      } else {
        rets.push_back(std::move(msg));
      }
    }
    too_fast_buffer_.clear();
  }

  std::list<Message> get_messages = Pop(progress, sender);
  VLOG(1) << "progress: " << progress << " sender: " << sender
    << " get_messages size " << get_messages.size();
  auto get_msg_iter = get_messages.begin();
  while(get_msg_iter != get_messages.end()) {
    VLOG(1) << "message version: " << (*get_msg_iter).meta.version << " message sender: " << (*get_msg_iter).meta.sender;
    CHECK(((*get_msg_iter).meta.version > min_clock) || ((*get_msg_iter).meta.version < min_clock + staleness_ + speculation_))
        << "[Error]SparseSSPModel: progress invalid";
    CHECK((*get_msg_iter).data.size() == 1);
    if ((*get_msg_iter).meta.version <= staleness_ + min_clock) {
      rets.push_back(std::move(*get_msg_iter));
      get_msg_iter ++;
      VLOG(1) << "Not in speculation zone! Satisfied by stalenss";
    } 
    else if ((*get_msg_iter).meta.version <= staleness_ + speculation_ + min_clock) {
      VLOG(1) << "In speculation zone! "
        << "min_clock " << min_clock << " check_biggest_version " << (*get_msg_iter).meta.version - staleness_ - 1;
      int forwarded_worker_id = -1;
      int forwarded_version = -1;
      if (HasConflict(third_party::SArray<uint32_t>((*get_msg_iter).data[0]), min_clock,
                                   (*get_msg_iter).meta.version - staleness_ - 1, forwarded_worker_id, forwarded_version)) {
        CHECK(forwarded_version >= min_clock) << "[Error]SparseSSPModel: forwarded_version invalid";
        Push(forwarded_version, (*get_msg_iter), forwarded_worker_id);
        get_msg_iter = get_messages.erase(get_msg_iter);
        VLOG(1) << "Conflict, forwarded to version: " << forwarded_version << " worker_id: " << forwarded_worker_id;
      } 
      else {
        rets.push_back(std::move(*get_msg_iter));
        get_msg_iter ++;
        VLOG(1) << "No Conflict, Satisfied by sparsity";
      }
    }
    else if ((*get_msg_iter).meta.version == staleness_ + speculation_ + min_clock + 1) {
      too_fast_buffer_.push_back(std::move(*get_msg_iter));
      get_msg_iter = get_messages.erase(get_msg_iter);
    }
    else {
      CHECK(false) << " version: " << (*get_msg_iter).meta.version << " tid: " << (*get_msg_iter).meta.sender << " current clock tid: " << sender << " version: " << progress;
    }
  }

  if (updated_min_clock != -1) {  // min clock updated
    if (updated_min_clock == 1) {  // for the first version, no one is clearing the buffer
      buffer_.erase(0);
    }
    CHECK(Size(updated_min_clock - 1) == 0)
        << "[Error]SparseSSPModel: get_buffer_ of min_clock should empty";
    CHECK(TotalSize(updated_min_clock - 1) == 0)
        << "[Error]SparseSSPModel: Recorder Size should be 0";
    ClockRemoveRecord(updated_min_clock - 1);
    VLOG(1) << "Min clock updated to progress: " << progress;
  }
  return rets;
}


void SparseSSPController::AddRecord(Message& msg) {
  CHECK_LT(future_keys_[msg.meta.sender].size(), speculation_ + 1);
  future_keys_[msg.meta.sender].push({msg.meta.version, third_party::SArray<Key>(msg.data[0])});

  for (auto& key : third_party::SArray<uint32_t>(msg.data[0])) {
    recorder_[msg.meta.version][key].insert(msg.meta.sender);
  }

  Push(msg.meta.version, msg, msg.meta.sender);

}

std::list<Message> SparseSSPController::Pop(const int version, const int tid) {
  std::list<Message> ret = std::move(buffer_[version][tid]);
  buffer_[version].erase(tid);
  if (buffer_[version].empty()) {
    buffer_.erase(version);
  }
  return ret;
}

void SparseSSPController::Push(const int version, Message& message, const int tid) {
  // CHECK_EQ(buffer_[version][tid].size(), 0);
  buffer_[version][tid].push_back(std::move(message));
}


int SparseSSPController::Size(const int version) {
  if (buffer_.find(version) == buffer_.end()) {
    return 0;
  }
  int size = 0;
  for (auto& tid_messages : buffer_[version]) {
    size += tid_messages.second.size();
  }
  return size;
}

/* IF:
 *   NO conflict: return 
 *   ONE or SEVERAL conflict: append to the corresponding get buffer, return true
 */
bool SparseSSPController::HasConflict(const third_party::SArray<uint32_t>& paramIDs, const int begin_version,
                                          const int end_version, int& forwarded_thread_id, int& forwarded_version) {
  for (int check_version = end_version; check_version >= begin_version; check_version--) {
    for (auto& key : paramIDs) {
      auto it = recorder_[check_version].find(key);
      if (it != recorder_[check_version].end()) {
        forwarded_thread_id = *((it->second).begin());
        forwarded_version = check_version + 1;
        return true;
      }
    }
  }
  return false;
}

void SparseSSPController::RemoveRecord(const int version, const uint32_t tid,
                                          const third_party::SArray<uint32_t>& paramIDs) {
  CHECK(recorder_.find(version) != recorder_.end());
  for (auto& key : paramIDs) {
    CHECK(recorder_[version].find(key) != recorder_[version].end());
    CHECK(recorder_[version][key].find(tid) != recorder_[version][key].end());
    recorder_[version][key].erase(tid);
    CHECK(recorder_[version][key].size() >= 0);
    if (recorder_[version][key].size() == 0) {
      recorder_[version].erase(key);
    }
  }
}

void SparseSSPController::ClockRemoveRecord(const int version) { recorder_.erase(version); }

int SparseSSPController::ParamSize(const int version) {
  if (recorder_.find(version) == recorder_.end())
    return 0;
  return recorder_[version].size();
}

int SparseSSPController::WorkerSize(const int version) {
  if (recorder_.find(version) == recorder_.end())
    return 0;
  std::set<uint32_t> thread_id;
  for (auto& param_map : recorder_[version]) {
    for (auto& thread : param_map.second) {
      thread_id.insert(thread);
    }
  }
  return thread_id.size();
}

int SparseSSPController::TotalSize(const int version) {
  if (recorder_.find(version) == recorder_.end())
    return 0;
  int total_count = 0;
  for (auto& param_map : recorder_[version]) {
    for (auto& thread : param_map.second) {
      total_count++;
    }
  }
  return total_count;
}

}  // namespace flexps
