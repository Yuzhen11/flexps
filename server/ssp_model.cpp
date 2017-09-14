#include "server/ssp_model.hpp"
#include "glog/logging.h"

namespace flexps {

SSPModel::SSPModel(uint32_t model_id, std::unique_ptr<AbstractStorage>&& storage_ptr,
                   int staleness, ThreadsafeQueue<Message>* reply_queue)
    : model_id_(model_id), staleness_(staleness), reply_queue_(reply_queue) {
  this->storage_ = std::move(storage_ptr);
}

void SSPModel::ResetWorker(std::vector<int> tids) {
  this->progress_tracker_.Init(std::move(tids));
}

void SSPModel::Clock(Message& message) {
  int updated_min_clock = progress_tracker_.AdvanceAndGetChangedMinClock(message.meta.sender);
  if (updated_min_clock != -1) {  // min clock updated
    auto reqs_blocked_at_this_min_clock = buffer_.Pop(updated_min_clock);
    for (auto req : reqs_blocked_at_this_min_clock) {
      reply_queue_->Push(storage_->Get(req));
    }
    storage_->FinishIter();
  }
}

void SSPModel::Add(Message& message) {
  // The add will always never be blocked
  storage_->Add(message);
}

void SSPModel::Get(Message& message) {
  CHECK(progress_tracker_.CheckThreadValid(message.meta.sender));
  int progress = progress_tracker_.GetProgress(message.meta.sender);
  int min_clock = progress_tracker_.GetMinClock();
  if (progress - min_clock > staleness_) {
    buffer_.Push(progress - staleness_, message);
  } else {
    reply_queue_->Push(storage_->Get(message));
  }
}

int SSPModel::GetProgress(int tid) { return progress_tracker_.GetProgress(tid); }

int SSPModel::GetPendingSize(int progress) { return buffer_.Size(progress); }

}  // namespace flexps
