// #include "server/sparse_ssp_model.hpp"
// #include "glog/logging.h"

// namespace flexps {

// SparseSSPModel::SparseSSPModel(uint32_t model_id, std::unique_ptr<AbstractStorage>&& storage_ptr,
//                                ThreadsafeQueue<Message>* reply_queue, int staleness, int speculation)
//     : model_id_(model_id), reply_queue_(reply_queue), staleness_(staleness), speculation_(speculation) {
//   this->storage_ = std::move(storage_ptr);
//   // TODO(Ruoyu Wu): kind of ugly
//   this->get_buffer_ = std::unique_ptr<SparsePendingBuffer>(new SparsePendingBuffer());
//   this->detector_ = std::unique_ptr<SparseConflictDetector>(new SparseConflictDetector());
// }

// void SparseSSPModel::Clock(Message& message) {
//   int sender = message.meta.sender;
//   int updated_min_clock = progress_tracker_.AdvanceAndGetChangedMinClock(sender);
//   int min_clock = progress_tracker_.GetMinClock();
//   int progress = progress_tracker_.GetProgress(message.meta.sender);
//   std::vector<Message> get_messages = get_buffer_->Pop(progress, sender);

//   for (auto& get_message : get_messages) {
//     if (get_message.meta.version - min_clock <= staleness_) {
//       reply_queue_->Push(storage_->Get(get_message));
//     } else if (get_message.meta.version - min_clock <= staleness_ + speculation_) {
//       if (!detector_->ConflictInfo(get_message.data[0], min_clock, get_message.meta.version - staleness_)) {
//         reply_queue_->Push(storage_->Get(get_message));
//         detector_->RemoveRecord(progress - 1, get_message.data[0]);
//       } else {
//         // append to corresponding
//       }
//     } else {
//       CHECK(false) << "[Error]SparseSSPModel: progress is out of possible scope";
//     }
//   }

//   if (updated_min_clock != -1) {  // min clock updated
//     CHECK(get_buffer_->Size(updated_min_clock - 1) == 0)
//         << "[Error]SparseSSPModel: get_buffer_ of min_clock should empty";
//     CHECK(detector_->RecorderSize(updated_min_clock - 1) == 0) << "[Error]SparseSSPModel: Recorder Size should be 0";
//     detector_->ClockRemoveRecord(updated_min_clock - 1);
//     storage_->FinishIter();
//   }
// }

// void SparseSSPModel::Get(Message& message) {
//   CHECK(progress_tracker_.CheckThreadValid(message.meta.sender));
//   CHECK(message.data.size() == 2);
//   detector_->AddRecord(message.meta.version, message.data[0]);
//   // Multiple messages might be sent from the client
//   if (message.meta.version == 0) {
//     reply_queue_->Push(storage_->Get(message));
//   } else {
//     get_buffer_->Push(message.meta.version, message, message.meta.sender);
//   }
// }

// void SparseSSPModel::Add(Message& message) { storage_->Add(message); }

// void SparseSSPModel::ResetWorker(const std::vector<uint32_t>& tids) { this->progress_tracker_.Init(tids); }

// int SparseSSPModel::GetProgress(int tid) { return progress_tracker_.GetProgress(tid); }

// }  // namespace flexps
