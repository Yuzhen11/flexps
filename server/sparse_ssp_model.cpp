#include "server/sparse_ssp_model.hpp"
#include "glog/logging.h"

#include <iostream>

namespace flexps {

SparseSSPModel::SparseSSPModel(const uint32_t model_id, std::unique_ptr<AbstractStorage>&& storage_ptr,
                               ThreadsafeQueue<Message>* reply_queue, const int staleness, const int speculation)
    : model_id_(model_id), reply_queue_(reply_queue), staleness_(staleness), speculation_(speculation) {
  this->storage_ = std::move(storage_ptr);
  this->get_buffer_ = std::unique_ptr<AbstractPendingBuffer>(new SparsePendingBuffer());
  this->detector_ = std::unique_ptr<AbstractConflictDetector>(new SparseConflictDetector());
}

void SparseSSPModel::Clock(Message& message) {
  int sender = message.meta.sender;
  int updated_min_clock = progress_tracker_.AdvanceAndGetChangedMinClock(sender);
  int min_clock = progress_tracker_.GetMinClock();
  int progress = progress_tracker_.GetProgress(message.meta.sender);
  // Although we assume only one get message in a version, multiple messages can still be poped out
  std::vector<Message> get_messages = get_buffer_->Pop(progress, sender);

  for (auto& get_message : get_messages) {
    CHECK((get_message.meta.version > min_clock) || (get_message.meta.version < min_clock + staleness_ + speculation_))
        << "[Error]SparseSSPModel: progress invalid";
    CHECK(get_message.data.size() == 1);
    if (get_message.meta.version - min_clock <= staleness_) {
      reply_queue_->Push(storage_->Get(get_message));
    } else if (get_message.meta.version - min_clock <= staleness_ + speculation_) {
      int forwarded_worker_id = -1;
      int forwarded_version = -1;
      if (!detector_->ConflictInfo(third_party::SArray<uint32_t>(get_message.data[0]), min_clock,
                                   get_message.meta.version - staleness_, forwarded_worker_id, forwarded_version)) {
        reply_queue_->Push(storage_->Get(get_message));
        detector_->RemoveRecord(get_message.meta.version, get_message.meta.sender,
                                third_party::SArray<uint32_t>(get_message.data[0]));
      } else {
        CHECK(forwarded_version >= min_clock) << "[Error]SparseSSPModel: forwarded_version invalid";
        CHECK(progress_tracker_.CheckThreadValid(forwarded_worker_id))
            << "[Error]SparseSSPModel: forwarded_worker_id invalid";
        get_buffer_->Push(forwarded_version, get_message, forwarded_worker_id);
      }
    }
  }

  if (updated_min_clock != -1) {  // min clock updated
    CHECK(get_buffer_->Size(updated_min_clock - 1) == 0)
        << "[Error]SparseSSPModel: get_buffer_ of min_clock should empty";
    CHECK(dynamic_cast<SparseConflictDetector*>(detector_.get())->TotalSize(updated_min_clock - 1) == 0)
        << "[Error]SparseSSPModel: Recorder Size should be 0";
    detector_->ClockRemoveRecord(updated_min_clock - 1);
    storage_->FinishIter();
  }
}

void SparseSSPModel::Get(Message& message) {
  CHECK(progress_tracker_.CheckThreadValid(message.meta.sender));
  CHECK(message.data.size() == 1);
  detector_->AddRecord(message.meta.version, message.meta.sender, third_party::SArray<uint32_t>(message.data[0]));
  if (message.meta.version == 0) {
    reply_queue_->Push(storage_->Get(message));
  } else {
    get_buffer_->Push(message.meta.version, message, message.meta.sender);
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
