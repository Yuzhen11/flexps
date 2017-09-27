#include "server/sparse_ssp_model.hpp"

#include "glog/logging.h"

namespace flexps {

SparseSSPModel::SparseSSPModel(const uint32_t model_id, std::unique_ptr<AbstractStorage>&& storage, 
                               std::unique_ptr<AbstractSparseSSPRecorder> recorder,
                               ThreadsafeQueue<Message>* reply_queue, int staleness, int speculation)
    : model_id_(model_id), reply_queue_(reply_queue),
    storage_(std::move(storage)),
    recorder_(std::move(recorder)),
    staleness_(staleness), speculation_(speculation) {}

void SparseSSPModel::Clock(Message& message) {
  int min_clock = progress_tracker_.GetMinClock();
  
  // If the Clock is too fast, store it in buffer_ first. Handle this Clock when min_clock is updated.
  if (message.meta.version >= staleness_ + speculation_ + min_clock + 1) {
    CHECK_EQ(message.meta.version, staleness_ + speculation_ + min_clock + 1);
    buffer_.push_back(std::move(message));
    return;
  }

  int sender = message.meta.sender;
  int updated_min_clock = progress_tracker_.AdvanceAndGetChangedMinClock(sender);
  int progress = progress_tracker_.GetProgress(message.meta.sender);

  // TODO(Ruoyu Wu): comment
  recorder_->HandleFutureKeys(progress, sender);

  // Although we assume only one get message in a version, multiple messages can be in get_messages list
  std::list<Message> get_messages;

  // TODO(Ruoyu Wu): comment
  recorder_->HandleTooFastBuffer(updated_min_clock, min_clock, get_messages);

  std::list<Message> pop_messages = recorder_->PopMsg(progress, sender);
  VLOG(1) << "progress: " << progress << " sender: " << sender
    << " pop_messages size " << pop_messages.size();
  auto pop_msg_iter = pop_messages.begin();
  while(pop_msg_iter != pop_messages.end()) {
    VLOG(1) << "message version: " << (*pop_msg_iter).meta.version << " message sender: " << (*pop_msg_iter).meta.sender;
    CHECK(((*pop_msg_iter).meta.version > min_clock) || ((*pop_msg_iter).meta.version < min_clock + staleness_ + speculation_))
        << "[Error]SparseSSPModel: progress invalid";
    CHECK((*pop_msg_iter).data.size() == 1);
    if ((*pop_msg_iter).meta.version <= staleness_ + min_clock) {
      get_messages.push_back(std::move(*pop_msg_iter));
      pop_msg_iter ++;
      VLOG(1) << "Not in speculation zone! Satisfied by stalenss";
    } 
    else if ((*pop_msg_iter).meta.version <= staleness_ + speculation_ + min_clock) {
      VLOG(1) << "In speculation zone! "
        << "min_clock " << min_clock << " check_biggest_version " << (*pop_msg_iter).meta.version - staleness_ - 1;
      int forwarded_worker_id = -1;
      int forwarded_version = -1;
      if (recorder_->HasConflict(third_party::SArray<uint32_t>((*pop_msg_iter).data[0]), min_clock,
                                   (*pop_msg_iter).meta.version - staleness_ - 1, forwarded_worker_id, forwarded_version)) {
        CHECK(forwarded_version >= min_clock) << "[Error]SparseSSPModel: forwarded_version invalid";
        recorder_->PushMsg(forwarded_version, (*pop_msg_iter), forwarded_worker_id);
        pop_msg_iter = pop_messages.erase(pop_msg_iter);
        VLOG(1) << "Conflict, forwarded to version: " << forwarded_version << " worker_id: " << forwarded_worker_id;
      } 
      else {
        get_messages.push_back(std::move(*pop_msg_iter));
        pop_msg_iter ++;
        VLOG(1) << "No Conflict, Satisfied by sparsity";
      }
    }
    else if ((*pop_msg_iter).meta.version == staleness_ + speculation_ + min_clock + 1) {
      recorder_->PushBackTooFastBuffer((*pop_msg_iter));
      pop_msg_iter = pop_messages.erase(pop_msg_iter);
    }
    else {
      CHECK(false) << " version: " << (*pop_msg_iter).meta.version << " tid: " << (*pop_msg_iter).meta.sender << " current clock tid: " << sender << " version: " << progress;
    }
  }

  if (updated_min_clock != -1) {  // min clock updated
    if (updated_min_clock == 1) {  // for the first version, no one is clearing the buffer
      recorder_->EraseMsgBuffer(0);
    }
    CHECK(recorder_->MsgBufferSize(updated_min_clock - 1) == 0)
        << "[Error]SparseSSPModel: get_buffer_ of min_clock should empty";
    // CHECK(TotalSize(updated_min_clock - 1) == 0)
    //     << "[Error]SparseSSPModel: Recorder Size should be 0";
    recorder_->ClockRemoveRecord(updated_min_clock - 1);
    VLOG(1) << "Min clock updated to progress: " << progress;
  }

  for (auto& msg : get_messages) {
    CHECK(msg.data.size() == 1);
    reply_queue_->Push(storage_->Get(msg));
  }

  if (updated_min_clock != -1) {  // min clock updated
    storage_->FinishIter();
    for (auto& msg : buffer_) {
      Clock(msg);
    }
    buffer_.clear();
  }
}

void SparseSSPModel::Get(Message& message) {
  CHECK(progress_tracker_.CheckThreadValid(message.meta.sender));
  CHECK(message.data.size() == 1);
  if (message.meta.version == 0) {
    reply_queue_->Push(storage_->Get(message));
  }

  recorder_->AddRecord(message);
  recorder_->PushMsg(message.meta.version, message, message.meta.sender);
}

void SparseSSPModel::Add(Message& message) {
  CHECK(message.meta.version == progress_tracker_.GetProgress(message.meta.sender))
      << "[Error]SparseSSPModel: Add version invalid";
  storage_->Add(message);
}

void SparseSSPModel::ResetWorker(Message& msg) {
  CHECK_EQ(msg.data.size(), 1);
  third_party::SArray<uint32_t> tids;
  tids = msg.data[0];
  std::vector<uint32_t> tids_vec;
  for (auto tid : tids)
    tids_vec.push_back(tid);
  this->progress_tracker_.Init(tids_vec);
  Message reply_msg;
  reply_msg.meta.model_id = model_id_;
  reply_msg.meta.recver = msg.meta.sender;
  reply_msg.meta.flag = Flag::kResetWorkerInModel;
  reply_queue_->Push(reply_msg);
}

int SparseSSPModel::GetProgress(int tid) { return progress_tracker_.GetProgress(tid); }

}  // namespace flexps
