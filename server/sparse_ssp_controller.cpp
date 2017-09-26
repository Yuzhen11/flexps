#include "server/sparse_ssp_controller.hpp"
#include "server/abstract_sparse_ssp_recorder.hpp"
#include "server/unordered_map_sparse_ssp_recorder.hpp"

#include "glog/logging.h"

#include <iostream>
#include <sstream>

namespace flexps {

SparseSSPController::SparseSSPController(uint32_t staleness, uint32_t speculation)
    : staleness_(staleness), speculation_(speculation) {
  this->recorder_ = std::unique_ptr<AbstractSparseSSPRecorder>(new UnorderedMapSparseSSPRecorder(staleness, speculation));
}

std::list<Message> SparseSSPController::Clock(int progress, int sender, int updated_min_clock, int min_clock) {

  // TODO(Ruoyu Wu): comment
  recorder_->HandleFutureKeys(progress, sender);

  std::list<Message> rets;

  // TODO(Ruoyu Wu): comment
  HandleTooFastBuffer(updated_min_clock, min_clock, rets);

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
      if (recorder_->HasConflict(third_party::SArray<uint32_t>((*get_msg_iter).data[0]), min_clock,
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
    // CHECK(TotalSize(updated_min_clock - 1) == 0)
    //     << "[Error]SparseSSPModel: Recorder Size should be 0";
    recorder_->ClockRemoveRecord(updated_min_clock - 1);
    VLOG(1) << "Min clock updated to progress: " << progress;
  }
  return rets;
}

void SparseSSPController::AddRecord(Message& msg) {
  recorder_->AddRecord(msg);
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

void SparseSSPController::HandleTooFastBuffer(int updated_min_clock, int min_clock, std::list<Message>& rets) {
  // Handle too_fast_buffer_ if min_clock updated
  // Maybe it's clear to move this logic to handle updated_min_clock out.
  if (updated_min_clock != -1) {
    for (auto& msg : too_fast_buffer_) {
      int forwarded_worker_id = -1;
      int forwarded_version = -1;
      if (recorder_->HasConflict(third_party::SArray<uint32_t>(msg.data[0]), min_clock,
            msg.meta.version - staleness_ - 1, forwarded_worker_id, forwarded_version)) {
        CHECK(forwarded_version >= min_clock) << "[Error]SparseSSPModel: forwarded_version invalid";
        Push(forwarded_version, msg, forwarded_worker_id);
      } else {
        rets.push_back(std::move(msg));
      }
    }
    too_fast_buffer_.clear();
  }
}


}  // namespace flexps
