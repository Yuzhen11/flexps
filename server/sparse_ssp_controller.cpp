#include "server/sparse_ssp_controller.hpp"

#include "glog/logging.h"

namespace flexps {

std::vector<Message> SparseSSPController::UnblockRequests(int progress, int sender, int updated_min_clock, int min_clock) {
  std::vector<Message> rets;
  std::vector<Message> get_messages = get_buffer_->Pop(progress, sender);
  VLOG(1) << "progress: " << progress << " sender: " << sender;
  for (auto& get_message : get_messages) {
    CHECK((get_message.meta.version > min_clock) || (get_message.meta.version < min_clock + staleness_ + speculation_))
        << "[Error]SparseSSPModel: progress invalid";
    CHECK(get_message.data.size() == 1);
    if (get_message.meta.version <= staleness_ + min_clock) {
      detector_->RemoveRecord(get_message.meta.version, get_message.meta.sender,
                                third_party::SArray<uint32_t>(get_message.data[0]));
      rets.push_back(std::move(get_message));
      VLOG(1) << "Satisfied by stalenss";
    } else if (get_message.meta.version <= staleness_ + speculation_ + min_clock) {
      int forwarded_worker_id = -1;
      int forwarded_version = -1;
      if (!detector_->ConflictInfo(third_party::SArray<uint32_t>(get_message.data[0]), min_clock,
                                   get_message.meta.version - staleness_, forwarded_worker_id, forwarded_version)) {
        detector_->RemoveRecord(get_message.meta.version, get_message.meta.sender,
                                third_party::SArray<uint32_t>(get_message.data[0]));
        rets.push_back(std::move(get_message));
        VLOG(1) << "Satisfied by sparsity";
      } else {
        CHECK(forwarded_version >= min_clock) << "[Error]SparseSSPModel: forwarded_version invalid";
        get_buffer_->Push(forwarded_version, get_message, forwarded_worker_id);
        VLOG(1) << "forwarded to version: " << forwarded_version << " worker_id: " << forwarded_worker_id;
      }
    }
  }

  if (updated_min_clock != -1) {  // min clock updated
    CHECK(get_buffer_->Size(updated_min_clock - 1) == 0)
        << "[Error]SparseSSPModel: get_buffer_ of min_clock should empty";
    CHECK(dynamic_cast<SparseConflictDetector*>(detector_.get())->TotalSize(updated_min_clock - 1) == 0)
        << "[Error]SparseSSPModel: Recorder Size should be 0";
    detector_->ClockRemoveRecord(updated_min_clock - 1);
  }
  return rets;
}


void SparseSSPController::AddRecord(Message& msg) {
  detector_->AddRecord(msg.meta.version, msg.meta.sender, third_party::SArray<uint32_t>(msg.data[0]));
  get_buffer_->Push(msg.meta.version, msg, msg.meta.sender);
}

}  // namespace flexps
