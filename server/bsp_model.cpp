#include "server/bsp_model.hpp"
#include "glog/logging.h"

namespace flexps {

BSPModel::BSPModel(uint32_t model_id, std::unique_ptr<AbstractStorage>&& storage_ptr,
                    ThreadsafeQueue<Message>* reply_queue)
    : model_id_(model_id), reply_queue_(reply_queue) {
  this->storage_ = std::move(storage_ptr);
}

void BSPModel::Clock(Message& message) {
  int updated_min_clock = progress_tracker_.AdvanceAndGetChangedMinClock(message.meta.sender);
  if (updated_min_clock != -1) {  // min clock updated
    for (auto add_req : add_buffer_) {
      storage_->Add(add_req);
    }
    add_buffer_.clear();

    for (auto get_req : get_buffer_) {
      reply_queue_->Push(storage_->Get(get_req));
    }
    get_buffer_.clear();

    storage_->FinishIter();
  }
}

void BSPModel::Add(Message& message) {
  CHECK(progress_tracker_.CheckThreadValid(message.meta.sender));
  int progress = progress_tracker_.GetProgress(message.meta.sender);
  if (progress == progress_tracker_.GetMinClock()) {
    add_buffer_.push_back(message);
  } 
  else {
    CHECK(false) << "progress error in BSPModel::Add";
  }
}

void BSPModel::Get(Message& message) {
  CHECK(progress_tracker_.CheckThreadValid(message.meta.sender));
  int progress = progress_tracker_.GetProgress(message.meta.sender);
  if (progress == progress_tracker_.GetMinClock() + 1) {
    get_buffer_.push_back(message);
  } 
  else if (progress == progress_tracker_.GetMinClock()){
    reply_queue_->Push(storage_->Get(message));
  }
  else {
    CHECK(false) << "progress error in BSPModel::Get";
  }
}

int BSPModel::GetProgress(int tid) { return progress_tracker_.GetProgress(tid); }

int BSPModel::GetGetPendingSize() { return get_buffer_.size(); }

int BSPModel::GetAddPendingSize() { return add_buffer_.size(); }

void BSPModel::ResetWorker(const std::vector<uint32_t> tids) {
  this->progress_tracker_.Init(tids);
}

}  // namespace flexps
