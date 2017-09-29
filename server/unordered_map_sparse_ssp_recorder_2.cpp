#include "server/unordered_map_sparse_ssp_recorder_2.hpp"
#include "glog/logging.h"

namespace flexps {

std::vector<Message> UnorderedMapSparseSSPRecorder2::GetNonConflictMsgs(int progress, int sender, int min_clock) {
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
        main_recorder_[forwarded_version][forwarded_key].second.push_back(std::move(msg));
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

std::vector<Message> UnorderedMapSparseSSPRecorder2::HandleTooFastBuffer(int min_clock) {
  std::vector<Message> rets;
  for (auto& msg : too_fast_buffer_) {
    int forwarded_key = -1;
    int forwarded_version = -1;
    if (HasConflict(third_party::SArray<uint32_t>(msg.data[0]), min_clock,
          msg.meta.version - staleness_ - 1, &forwarded_key, &forwarded_version)) {
      main_recorder_[forwarded_version][forwarded_key].second.push_back(std::move(msg));
    } else {
      rets.push_back(std::move(msg));
    }
  }
  too_fast_buffer_.clear();
  return rets;
}

void UnorderedMapSparseSSPRecorder2::AddRecord(Message& msg) {
  DCHECK_LT(future_keys_[msg.meta.sender].size(), speculation_ + 1);
  future_keys_[msg.meta.sender].push({msg.meta.version, third_party::SArray<Key>(msg.data[0])});

  for (auto& key : third_party::SArray<uint32_t>(msg.data[0])) {
    main_recorder_[msg.meta.version][key].first += 1;
  }

  int version = msg.meta.version;
  if (version != 0) {
    future_msgs_[msg.meta.sender].push({version, std::move(msg)});
  }
}

std::vector<Message> UnorderedMapSparseSSPRecorder2::RemoveRecordAndGetNonConflictMsgs(int version, int min_clock, uint32_t tid,
                                          const third_party::SArray<uint32_t>& keys) {
  std::vector<Message> msgs_to_be_handled;
  DCHECK(main_recorder_.find(version) != main_recorder_.end());
  for (auto& key : keys) {
    DCHECK(main_recorder_[version].find(key) != main_recorder_[version].end());
    DCHECK_GE(main_recorder_[version][key].first, 0);
    main_recorder_[version][key].first -= 1;
    if (main_recorder_[version][key].first == 0) {
      for (auto& msg : main_recorder_[version][key].second) {
        msgs_to_be_handled.push_back(std::move(msg));
      }
      main_recorder_[version].erase(key);
    }
  }
  std::vector<Message> msgs_to_be_replied;
  for (auto& msg : msgs_to_be_handled) {
    int forwarded_key = -1;
    int forwarded_version = -1;
    if (HasConflict(third_party::SArray<Key>(msg.data[0]), min_clock, msg.meta.version - staleness_ - 1,
           &forwarded_key, &forwarded_version)) {
      main_recorder_[forwarded_version][forwarded_key].second.push_back(std::move(msg));
    } else {
      msgs_to_be_replied.push_back(std::move(msg));
    }
  }
  return msgs_to_be_replied;
}

void UnorderedMapSparseSSPRecorder2::RemoveRecord(const int version) { 
  main_recorder_.erase(version); 
}

/* IF:
 *   NO conflict: return 
 *   ONE or SEVERAL conflict: append to the corresponding get buffer, return true
 */
bool UnorderedMapSparseSSPRecorder2::HasConflict(const third_party::SArray<uint32_t>& keys, const int begin_version,
                                          const int end_version, int* forwarded_key, int* forwarded_version) {
  for (int check_version = end_version; check_version >= begin_version; check_version--) {
    for (auto& key : keys) {
      auto it = main_recorder_[check_version].find(key);
      if (it != main_recorder_[check_version].end()) {
        *forwarded_key = key;
        *forwarded_version = check_version;
        return true;
      }
    }
  }
  return false;
}

} // namespace flexps
