#include "server/sparse_ssp_model.hpp"
#include "glog/logging.h"

#include <iostream>

namespace flexps {

SparseSSPModel::SparseSSPModel(const uint32_t model_id, std::unique_ptr<AbstractStorage>&& storage,
                               ThreadsafeQueue<Message>* reply_queue, int staleness, int speculation)
    : model_id_(model_id), reply_queue_(reply_queue),
    storage_(std::move(storage)),
    sparse_ssp_controller_(staleness, speculation),
    staleness_(staleness), speculation_(speculation) {
}

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

  // Although we assume only one get message in a version, multiple messages can still be poped out
  std::list<Message> get_messages = sparse_ssp_controller_.Clock(progress, sender, updated_min_clock, min_clock);
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
  sparse_ssp_controller_.AddRecord(message);
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
