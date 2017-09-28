#include "server/vector_sparse_ssp_recorder_2.hpp"
#include "glog/logging.h"

namespace flexps {

VectorSparseSSPRecorder2::VectorSparseSSPRecorder2(uint32_t staleness, uint32_t speculation, third_party::Range range) 
    : staleness_(staleness), speculation_(speculation), range_(range) {

  // Just make sure that preallocated space is larger than needed
  main_recorder_version_level_size_ = staleness_ + 2 * speculation_ + 3;

  main_recorder_.resize(main_recorder_version_level_size_);

  for (auto& version_recorder : main_recorder_) {
    DCHECK(range_.end() > range_.begin());
    version_recorder.resize(range_.end() - range_.begin());
  }
}

std::vector<Message> VectorSparseSSPRecorder2::GetNonConflictMsgs(int progress, int sender, int min_clock) {
  std::vector<Message> msgs;
  // Get() that are block here
  if (future_keys_[sender].size() > 0 && future_keys_[sender].front().first == progress - 1) {
    msgs = RemoveRecordAndGetNonConflictMsgs(progress - 1, min_clock, sender, future_keys_[sender].front().second);
    future_keys_[sender].pop();
  }
  // Its own Get()
  if (future_msgs_[sender].size() > 0 && future_msgs_[sender].front().first == progress) {
    Message& msg = future_msgs_[sender].front().second;
    CHECK(msg.meta.version >= min_clock && msg.meta.version < min_clock + staleness_ + speculation_ + 2)
        << "msg version: " << msg.meta.version << " min_clock: " << min_clock << " staleness: " << staleness_ << " speculation: " << speculation_ ;
    if (msg.meta.version <= staleness_ + min_clock) {
      msgs.push_back(std::move(msg));
    } else if (msg.meta.version <= min_clock + staleness_ + speculation_) {
      int forwarded_key = -1;
      int forwarded_version = -1;
      if (HasConflict(third_party::SArray<Key>(msg.data[0]), min_clock, 
             msg.meta.version - staleness_ - 1, &forwarded_key, &forwarded_version)) {
        main_recorder_[forwarded_version % main_recorder_version_level_size_][forwarded_key - range_.begin()].second.push_back(std::move(msg));
      } else {
        msgs.push_back(std::move(msg));
      }
    } else if (msg.meta.version == min_clock + staleness_ + speculation_ + 1) {
      too_fast_buffer_.push_back(std::move(msg));
    } else {
      CHECK(false) << " version: " << msg.meta.version << " tid: " << msg.meta.sender;
    }
    future_msgs_[sender].pop();
  }
  return msgs;
}

std::vector<Message> VectorSparseSSPRecorder2::HandleTooFastBuffer(int min_clock) {
  std::vector<Message> rets;
  for (auto& msg : too_fast_buffer_) {
    int forwarded_key = -1;
    int forwarded_version = -1;
    if (HasConflict(third_party::SArray<uint32_t>(msg.data[0]), min_clock,
          msg.meta.version - staleness_ - 1, &forwarded_key, &forwarded_version)) {
      main_recorder_[forwarded_version % main_recorder_version_level_size_][forwarded_key - range_.begin()].second.push_back(std::move(msg));
    } else {
      rets.push_back(std::move(msg));
    }
  }
  too_fast_buffer_.clear();
  return rets;
}

void VectorSparseSSPRecorder2::AddRecord(Message& msg) {
  DCHECK_LT(future_keys_[msg.meta.sender].size(), speculation_ + 1);
  future_keys_[msg.meta.sender].push({msg.meta.version, third_party::SArray<Key>(msg.data[0])});

  for (auto& key : third_party::SArray<uint32_t>(msg.data[0])) {
    main_recorder_[msg.meta.version % main_recorder_version_level_size_][key - range_.begin()].first += 1;
  }

  int version = msg.meta.version;
  if (version != 0) {
    future_msgs_[msg.meta.sender].push({version, std::move(msg)});
  }
}

std::vector<Message> VectorSparseSSPRecorder2::RemoveRecordAndGetNonConflictMsgs(int version, int min_clock, uint32_t tid,
                                          const third_party::SArray<uint32_t>& keys) {
  std::vector<Message> msgs_to_be_handled;
  for (auto& key : keys) {
    DCHECK_GE(main_recorder_[version % main_recorder_version_level_size_][key - range_.begin()].first, 0);
    main_recorder_[version % main_recorder_version_level_size_][key - range_.begin()].first -= 1;
    if (main_recorder_[version % main_recorder_version_level_size_][key - range_.begin()].first == 0) {
      for (auto& msg : main_recorder_[version % main_recorder_version_level_size_][key - range_.begin()].second) {
        msgs_to_be_handled.push_back(std::move(msg));
      }
      main_recorder_[version % main_recorder_version_level_size_][key - range_.begin()].second.clear();
    }
  }
  std::vector<Message> msgs_to_be_replied;
  for (auto& msg : msgs_to_be_handled) {
    int forwarded_key = -1;
    int forwarded_version = -1;
    if (HasConflict(third_party::SArray<Key>(msg.data[0]), min_clock, msg.meta.version - staleness_ - 1,
           &forwarded_key, &forwarded_version)) {
      main_recorder_[forwarded_version % main_recorder_version_level_size_][forwarded_key - range_.begin()].second.push_back(std::move(msg));
    } else {
      msgs_to_be_replied.push_back(std::move(msg));
    }
  }
  return msgs_to_be_replied;
}

void VectorSparseSSPRecorder2::RemoveRecord(const int version) { 
  // for (int i = 0; i < main_recorder_[version % main_recorder_version_level_size_].size(); ++ i) {
  //   CHECK_EQ(main_recorder_[version % main_recorder_version_level_size_][i].first, 0);
  // }
}

/* IF:
 *   NO conflict: return 
 *   ONE or SEVERAL conflict: append to the corresponding get buffer, return true
 */
bool VectorSparseSSPRecorder2::HasConflict(const third_party::SArray<uint32_t>& keys, const int begin_version,
                                          const int end_version, int* forwarded_key, int* forwarded_version) {
  for (int check_version = end_version; check_version >= begin_version; check_version--) {
    for (auto& key : keys) {
      if (main_recorder_[check_version % main_recorder_version_level_size_][key - range_.begin()].first > 0) {
        *forwarded_key = key;
        *forwarded_version = check_version;
        return true;
      }
    }
  }
  return false;
}

} // namespace flexps
