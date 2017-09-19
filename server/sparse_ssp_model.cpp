#include "server/sparse_ssp_model.hpp"
#include "glog/logging.h"

#include <iostream>

namespace flexps {

SparseSSPModel::SparseSSPModel(const uint32_t model_id, std::unique_ptr<AbstractStorage>&& storage,
                               ThreadsafeQueue<Message>* reply_queue, std::unique_ptr<AbstractSparseSSPController>&& sparse_ssp_controller)
    : model_id_(model_id), reply_queue_(reply_queue),
    storage_(std::move(storage)), sparse_ssp_controller_(std::move(sparse_ssp_controller)) {
}

void SparseSSPModel::Clock(Message& message) {
  int sender = message.meta.sender;
  int updated_min_clock = progress_tracker_.AdvanceAndGetChangedMinClock(sender);
  int min_clock = progress_tracker_.GetMinClock();
  int progress = progress_tracker_.GetProgress(message.meta.sender);
  // Although we assume only one get message in a version, multiple messages can still be poped out
  std::vector<Message> get_messages = sparse_ssp_controller_->UnblockRequests(progress, sender, updated_min_clock, min_clock);
  for (auto& msg : get_messages) {
    reply_queue_->Push(storage_->Get(msg));
  }

  if (updated_min_clock != -1) {  // min clock updated
    storage_->FinishIter();
  }
}

void SparseSSPModel::Get(Message& message) {
  CHECK(progress_tracker_.CheckThreadValid(message.meta.sender));
  CHECK(message.data.size() == 1);
  if (message.meta.version == 0) {
    reply_queue_->Push(storage_->Get(message));
  } else {
    sparse_ssp_controller_->AddRecord(message);
  }
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
