#include "server/vector_sparse_ssp_recorder.hpp"
#include "glog/logging.h"

namespace flexps {

VectorSparseSSPRecorder::VectorSparseSSPRecorder(uint32_t staleness, uint32_t speculation, third_party::Range range) 
  : staleness_(staleness), speculation_(speculation), range_(range) {

  // Just make sure that preallocated space is larger than needed
  main_recorder_version_level_size_ = staleness_ + 2 * speculation_ + 3;

  main_recorder_.resize(main_recorder_version_level_size_);

  for (auto& version_recorder : main_recorder_) {
    DCHECK(range_.end() > range_.begin());
    version_recorder.resize(range_.end() - range_.begin());
  }
}

void VectorSparseSSPRecorder::AddRecord(Message& msg) {
  DCHECK_LT(future_keys_[msg.meta.sender].size(), speculation_ + 1);
  future_keys_[msg.meta.sender].push({msg.meta.version, third_party::SArray<Key>(msg.data[0])});

  for (auto& key : third_party::SArray<uint32_t>(msg.data[0])) {
    DCHECK(key >= range_.begin() && key < range_.end());
    main_recorder_[msg.meta.version % main_recorder_version_level_size_][key - range_.begin()].insert(msg.meta.sender);
  }
}

void VectorSparseSSPRecorder::RemoveRecord(const int version, const uint32_t tid,
                                          const third_party::SArray<uint32_t>& paramIDs) {
  for (auto& key : paramIDs) {
    DCHECK(main_recorder_[version % main_recorder_version_level_size_].size() > key - range_.begin());
    DCHECK(key >= range_.begin() && key < range_.end());
    main_recorder_[version % main_recorder_version_level_size_][key - range_.begin()].erase(tid);
    DCHECK(main_recorder_[version % main_recorder_version_level_size_][key - range_.begin()].size() >= 0);
  }
}

void VectorSparseSSPRecorder::ClockRemoveRecord(const int version) {

  // // For test use
  // int count = 0;

  // // TODO(Ruoyu Wu): if logic is right, this func can be eliminated
  // for (auto& version_key_set : main_recorder_[version % main_recorder_version_level_size_]) {
  //   count += version_key_set.size();
  //   version_key_set.clear();
  // }

  // LOG(INFO) << version << " " << version % main_recorder_version_level_size_ << " CLOCK REMOVE SIZE " << count;
}

/* IF:
 *   NO conflict: return 
 *   ONE or SEVERAL conflict: append to the corresponding get buffer, return true
 */
bool VectorSparseSSPRecorder::HasConflict(const third_party::SArray<uint32_t>& paramIDs, const int begin_version,
                                          const int end_version, int& forwarded_thread_id, int& forwarded_version) {
  for (int check_version = end_version; check_version >= begin_version; check_version--) {
    for (auto& key : paramIDs) {
      DCHECK(key >= range_.begin() && key < range_.end());
      if (main_recorder_[check_version % main_recorder_version_level_size_][key - range_.begin()].size() > 0) {
        forwarded_thread_id = *(main_recorder_[check_version % main_recorder_version_level_size_][key - range_.begin()].begin());
        forwarded_version = check_version + 1;
        return true;
      } 
    }
  }
  return false;
}

void VectorSparseSSPRecorder::HandleFutureKeys(int progress, int sender) {
  if (future_keys_[sender].size() > 0 && future_keys_[sender].front().first == progress - 1) {
    RemoveRecord(progress - 1, sender, future_keys_[sender].front().second);
    future_keys_[sender].pop();
  }
}

void VectorSparseSSPRecorder::PushBackTooFastBuffer(Message& msg) {
  too_fast_buffer_.push_back(std::move(msg));
}

void VectorSparseSSPRecorder::EraseMsgBuffer(int version) {
  buffer_.erase(version);
}

std::list<Message> VectorSparseSSPRecorder::PopMsg(const int version, const int tid) {
  std::list<Message> ret = std::move(buffer_[version][tid]);
  buffer_[version].erase(tid);
  if (buffer_[version].empty()) {
    buffer_.erase(version);
  }
  return ret;
}

void VectorSparseSSPRecorder::PushMsg(const int version, Message& message, const int tid) {
  // CHECK_EQ(buffer_[version][tid].size(), 0);
  buffer_[version][tid].push_back(std::move(message));
}

int VectorSparseSSPRecorder::MsgBufferSize(const int version) {
  if (buffer_.find(version) == buffer_.end()) {
    return 0;
  }
  int size = 0;
  for (auto& tid_messages : buffer_[version]) {
    size += tid_messages.second.size();
  }
  return size;
}

void VectorSparseSSPRecorder::HandleTooFastBuffer(int updated_min_clock, int min_clock, std::list<Message>& rets) {
  // Handle too_fast_buffer_ if min_clock updated
  // Maybe it's clear to move this logic to handle updated_min_clock out.
  if (updated_min_clock != -1) {
    for (auto& msg : too_fast_buffer_) {
      int forwarded_worker_id = -1;
      int forwarded_version = -1;
      if (HasConflict(third_party::SArray<uint32_t>(msg.data[0]), min_clock,
            msg.meta.version - staleness_ - 1, forwarded_worker_id, forwarded_version)) {
        DCHECK(forwarded_version >= min_clock) << "[Error]SparseSSPModel: forwarded_version invalid";
        PushMsg(forwarded_version, msg, forwarded_worker_id);
      } else {
        rets.push_back(std::move(msg));
      }
    }
    too_fast_buffer_.clear();
  }
}

} // namespace flexps
