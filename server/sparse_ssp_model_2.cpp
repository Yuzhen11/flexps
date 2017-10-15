#include "server/sparse_ssp_model_2.hpp"

#include "glog/logging.h"

namespace flexps {

SparseSSPModel2::SparseSSPModel2(const uint32_t model_id, std::unique_ptr<AbstractStorage>&& storage, 
                               std::unique_ptr<AbstractSparseSSPRecorder2> recorder,
                               ThreadsafeQueue<Message>* reply_queue, int staleness, int speculation)
    : model_id_(model_id), reply_queue_(reply_queue),
    storage_(std::move(storage)),
    recorder_(std::move(recorder)),
    staleness_(staleness), speculation_(speculation) {}

void SparseSSPModel2::Clock(Message& message) {
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
  min_clock = progress_tracker_.GetMinClock();


  // 1. Remove records in the last round
  // If the key count in the recorder becomes 0, forward the message if there are still conflicts, 
  // reply the message otherwise.
  std::vector<Message> msgs;
  recorder_->GetNonConflictMsgs(progress, sender, min_clock, &msgs);
  for (auto& msg : msgs) {
    CHECK(msg.data.size() == 1);
    reply_queue_->Push(storage_->Get(msg));
  }
  
  // 2. Handle too fast buffer
  if (updated_min_clock != -1) {
    recorder_->RemoveRecord(updated_min_clock - 1);

    std::vector<Message> updated_min_clock_msgs;
    recorder_->HandleTooFastBuffer(updated_min_clock, &updated_min_clock_msgs);
    for (auto& msg : updated_min_clock_msgs) {
      CHECK(msg.data.size() == 1);
      reply_queue_->Push(storage_->Get(msg));
    }
  }

  if (updated_min_clock != -1) {
    storage_->FinishIter();
    for (auto& msg : buffer_) {
      Clock(msg);
    }
    buffer_.clear();
  }
}

void SparseSSPModel2::Get(Message& message) {
  CHECK(progress_tracker_.CheckThreadValid(message.meta.sender));
  CHECK(message.data.size() == 1);
  if (message.meta.version == 0) {
    reply_queue_->Push(storage_->Get(message));
  }

  recorder_->AddRecord(message);
}

void SparseSSPModel2::Add(Message& message) {
  CHECK(message.meta.version == progress_tracker_.GetProgress(message.meta.sender))
      << "[Error]SparseSSPModel2: Add version invalid";
  storage_->Add(message);
}

void SparseSSPModel2::ResetWorker(Message& msg) {
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

int SparseSSPModel2::GetProgress(int tid) { return progress_tracker_.GetProgress(tid); }

}  // namespace flexps
