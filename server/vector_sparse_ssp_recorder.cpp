#include "server/vector_sparse_ssp_recorder.hpp"
#include "glog/logging.h"

namespace flexps {

void VectorSparseSSPRecorder::AddRecord(Message& msg) {
  CHECK_LT(future_keys_[msg.meta.sender].size(), speculation_ + 1);
  future_keys_[msg.meta.sender].push({msg.meta.version, third_party::SArray<Key>(msg.data[0])});

  for (auto& key : third_party::SArray<uint32_t>(msg.data[0])) {
    main_recorder_[msg.meta.version][key - paramStart].insert(msg.meta.sender);
  }
}

void VectorSparseSSPRecorder::RemoveRecord(const int version, const uint32_t tid,
                                          const third_party::SArray<uint32_t>& paramIDs) {
  CHECK(main_recorder_.find(version) != main_recorder_.end());
  for (auto& key : paramIDs) {
    CHECK(main_recorder_[version].size() > key - paramStart);
    CHECK(main_recorder_[version][key - paramStart].find(tid) != main_recorder_[version][key - paramStart].end());
    main_recorder_[version][key - paramStart].erase(tid);
    CHECK(main_recorder_[version][key - paramStart].size() >= 0);
    // if (main_recorder_[version][key - paramStart].size() == 0) {
    //   main_recorder_[version].erase(key - paramStart);
    // }
  }
}

void VectorSparseSSPRecorder::ClockRemoveRecord(const int version) { 
  main_recorder_.erase(version); 
}

/* IF:
 *   NO conflict: return 
 *   ONE or SEVERAL conflict: append to the corresponding get buffer, return true
 */
bool VectorSparseSSPRecorder::HasConflict(const third_party::SArray<uint32_t>& paramIDs, const int begin_version,
                                          const int end_version, int& forwarded_thread_id, int& forwarded_version) {
  for (int check_version = end_version; check_version >= begin_version; check_version--) {
    for (auto& key : paramIDs) {
      if (main_recorder_[check_version][key - paramStart].size() > 0) {
        forwarded_thread_id = *(main_recorder_[check_version][key - paramStart].begin());
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
        CHECK(forwarded_version >= min_clock) << "[Error]SparseSSPModel: forwarded_version invalid";
        PushMsg(forwarded_version, msg, forwarded_worker_id);
      } else {
        rets.push_back(std::move(msg));
      }
    }
    too_fast_buffer_.clear();
  }
}

} // namespace flexps