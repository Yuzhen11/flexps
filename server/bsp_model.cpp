#include "server/bsp_model.hpp"
#include "glog/logging.h"

namespace flexps {

BSPModel::BSPModel(uint32_t model_id, std::unique_ptr<AbstractStorage>&& storage_ptr,
                   ThreadsafeQueue<Message>* reply_queue)
    : model_id_(model_id), reply_queue_(reply_queue) {
  this->storage_ = std::move(storage_ptr);
}

void BSPModel::Clock(Message& msg) {
  int updated_min_clock = progress_tracker_.AdvanceAndGetChangedMinClock(msg.meta.sender);
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

void BSPModel::Add(Message& msg) {
  CHECK(progress_tracker_.CheckThreadValid(msg.meta.sender));
  int progress = progress_tracker_.GetProgress(msg.meta.sender);
  if (progress == progress_tracker_.GetMinClock()) {
    add_buffer_.push_back(msg);
  } else {
    CHECK(false) << "progress error in BSPModel::Add";
  }
}

void BSPModel::Get(Message& msg) {
  CHECK(progress_tracker_.CheckThreadValid(msg.meta.sender));
  int progress = progress_tracker_.GetProgress(msg.meta.sender);
  if (progress == progress_tracker_.GetMinClock() + 1) {
    get_buffer_.push_back(msg);
  } else if (progress == progress_tracker_.GetMinClock()) {
    reply_queue_->Push(storage_->Get(msg));
  } else {
    CHECK(false) << "progress error in BSPModel::Get";
  }
}

int BSPModel::GetProgress(int tid) { return progress_tracker_.GetProgress(tid); }

int BSPModel::GetGetPendingSize() { return get_buffer_.size(); }

int BSPModel::GetAddPendingSize() { return add_buffer_.size(); }

void BSPModel::ResetWorker(Message& msg) {
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

}  // namespace flexps
