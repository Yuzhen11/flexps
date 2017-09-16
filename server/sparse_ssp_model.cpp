#include "server/sparse_ssp_model.hpp"
#include "glog/logging.h"

namespace flexps {

SparseSSPModel::SparseSSPModel(uint32_t model_id, std::unique_ptr<AbstractStorage>&& storage_ptr,
                    ThreadsafeQueue<Message>* reply_queue, int staleness, int speculation)
    : model_id_(model_id), reply_queue_(reply_queue), staleness_(staleness), speculation_(speculation) {
  this->storage_ = std::move(storage_ptr);
}

void SparseSSPModel::Clock(Message& message) {
  int sender = message.meta.sender;
  int updated_min_clock = progress_tracker_.AdvanceAndGetChangedMinClock(sender);
  int min_clock = progress_tracker_.GetMinClock();
  int progress = progress_tracker_.GetProgress(message.meta.sender);

  std::vector<Message> get_messages = get_buffer_.Pop(progress, sender);
  if (progress - min_clock <= staleness_) {
    for (auto& get_message : get_messages) {
      reply_queue_->Push(storage_->Get(get_message));
    }
  }
  else if (progress - min_clock <= staleness_ + speculation_) {
    for (auto& get_message : get_messages) {
      int latest_block_version = -1;
      int latest_block_thread_id = -1;
      if (GetConflictInfo(get_message, &latest_block_version, &latest_block_thread_id)) {
        get_buffer_.Push(latest_block_version, get_message, latest_block_thread_id);
      }
      else {
        reply_queue_->Push(storage_->Get(get_message));
      }
    }
  }
  else {
    CHECK(false) << "[Error]SparseSSPModel: progress is out of possible scope";
  }

  if (updated_min_clock != -1) {  // min clock updated
    CHECK(get_buffer_.Size(updated_min_clock - 1) == 0) << "[Error]SparseSSPModel: get_buffer_ of min_clock should empty";
    storage_->FinishIter();
  }
}

void SparseSSPModel::Get(Message& message) {
  CHECK(progress_tracker_.CheckThreadValid(message.meta.sender));
  if (message.meta.version == 0) {
    reply_queue_->Push(storage_->Get(message));
  }
  else {
    get_buffer_.Push(message.meta.version, message, message.meta.sender);
  }
}

void SparseSSPModel::Add(Message& message) {
  storage_->Add(message);
}

void SSPModel::ResetWorker(const std::vector<uint32_t> tids) {
  this->progress_tracker_.Init(tids);
}

int SparseSSPModel::GetProgress(int tid) { return progress_tracker_.GetProgress(tid); }

}  // namespace flexps
