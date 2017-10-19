#include "server/sparsessp/vector_sparse_ssp_recorder.hpp"
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

VectorSparseSSPRecorder::~VectorSparseSSPRecorder() {
#ifdef USE_TIMER
  LOG(INFO) << "add_record_time: " << add_record_time_.count()/1000. << " ms";
  LOG(INFO) << "remove_record_time: " << remove_record_time_.count()/1000. 
      << " ms";
  LOG(INFO) << "handle_own_get_time: " << handle_own_get_time_.count()/1000. << " ms";
  LOG(INFO) << "key_count: " << key_count_;
  LOG(INFO) << "by_staleness: " << by_staleness_
      << " forward: " << forward_
      << " by_speculation: " << by_speculation_
      << " too_fast: " << too_fast_
      << " total_forward: " << total_forward_;
#endif
}

void VectorSparseSSPRecorder::GetNonConflictMsgs(int progress, int sender, int min_clock, std::vector<Message>* const msgs) {
  // Get() that are block here
#ifdef USE_TIMER
  auto start_time = std::chrono::steady_clock::now();
#endif
  if (future_keys_[sender].size() > 0 && future_keys_[sender].front().first == progress - 1) {
    RemoveRecordAndGetNonConflictMsgs(progress - 1, min_clock, sender, future_keys_[sender].front().second, msgs);
    future_keys_[sender].pop();
  }
#ifdef USE_TIMER
  auto end_time = std::chrono::steady_clock::now();
  remove_record_time_ += std::chrono::duration_cast<std::chrono::microseconds>(end_time - start_time);
#endif

  // Its own Get()
#ifdef USE_TIMER
  start_time = std::chrono::steady_clock::now();
#endif
  if (future_msgs_[sender].size() > 0 && future_msgs_[sender].front().first == progress) {
    Message& msg = future_msgs_[sender].front().second;
    CHECK(msg.meta.version >= min_clock && msg.meta.version < min_clock + staleness_ + speculation_ + 2)
        << "msg version: " << msg.meta.version << " min_clock: " << min_clock << " staleness: " << staleness_ << " speculation: " << speculation_ ;
    if (msg.meta.version <= staleness_ + min_clock) {
      msgs->push_back(std::move(msg));
#ifdef USE_TIMER
      by_staleness_ += 1;
#endif
    } else if (msg.meta.version <= min_clock + staleness_ + speculation_) {
      int forwarded_key = -1;
      int forwarded_version = -1;
      if (HasConflict(third_party::SArray<Key>(msg.data[0]), min_clock, 
             msg.meta.version - staleness_ - 1, &forwarded_key, &forwarded_version)) {
        main_recorder_[forwarded_version % main_recorder_version_level_size_][forwarded_key - range_.begin()].second.push_back(std::move(msg));
#ifdef USE_TIMER
        forward_ += 1;
        total_forward_ += 1;
#endif
      } else {
        msgs->push_back(std::move(msg));
#ifdef USE_TIMER
        by_speculation_ += 1;
#endif
      }
    } else if (msg.meta.version == min_clock + staleness_ + speculation_ + 1) {
      too_fast_buffer_.push_back(std::move(msg));
#ifdef USE_TIMER
      too_fast_ += 1;
#endif
    } else {
      CHECK(false) << " version: " << msg.meta.version << " tid: " << msg.meta.sender;
    }
    future_msgs_[sender].pop();
  }
#ifdef USE_TIMER
  end_time = std::chrono::steady_clock::now();
  handle_own_get_time_ += std::chrono::duration_cast<std::chrono::microseconds>(end_time - start_time);
#endif
}

void VectorSparseSSPRecorder::HandleTooFastBuffer(int min_clock, std::vector<Message>* const msgs) {
  for (auto& msg : too_fast_buffer_) {
    int forwarded_key = -1;
    int forwarded_version = -1;
    if (HasConflict(third_party::SArray<Key>(msg.data[0]), min_clock,
          msg.meta.version - staleness_ - 1, &forwarded_key, &forwarded_version)) {
      main_recorder_[forwarded_version % main_recorder_version_level_size_][forwarded_key - range_.begin()].second.push_back(std::move(msg));
#ifdef USE_TIMER
      total_forward_ += 1;
#endif
    } else {
      msgs->push_back(std::move(msg));
    }
  }
  too_fast_buffer_.clear();
}

void VectorSparseSSPRecorder::AddRecord(Message& msg) {
  DCHECK_LT(future_keys_[msg.meta.sender].size(), speculation_ + 1);
  future_keys_[msg.meta.sender].push({msg.meta.version, third_party::SArray<Key>(msg.data[0])});

#ifdef USE_TIMER
  auto start_time = std::chrono::steady_clock::now();
#endif
  for (auto key : third_party::SArray<Key>(msg.data[0])) {
    main_recorder_[msg.meta.version % main_recorder_version_level_size_][key - range_.begin()].first += 1;
#ifdef USE_TIMER
    key_count_ += 1;
#endif
  }
#ifdef USE_TIMER
  auto end_time = std::chrono::steady_clock::now();
  add_record_time_ += std::chrono::duration_cast<std::chrono::microseconds>(end_time - start_time);
#endif

  int version = msg.meta.version;
  if (version != 0) {
    future_msgs_[msg.meta.sender].push({version, std::move(msg)});
  }
}

void VectorSparseSSPRecorder::RemoveRecordAndGetNonConflictMsgs(int version, int min_clock, uint32_t tid,
                                          const third_party::SArray<Key>& keys, std::vector<Message>* msgs) {
  std::vector<Message> msgs_to_be_handled;
  int version_after_mod = version % main_recorder_version_level_size_;
  for (auto key : keys) {
    int key_after_minus = key - range_.begin();
    DCHECK_GE(main_recorder_[version_after_mod][key_after_minus].first, 0);
    main_recorder_[version_after_mod][key_after_minus].first -= 1;
    if (main_recorder_[version_after_mod][key_after_minus].first == 0) {
      for (auto& msg : main_recorder_[version_after_mod][key_after_minus].second) {
        msgs_to_be_handled.push_back(std::move(msg));
      }
      main_recorder_[version_after_mod][key_after_minus].second.clear();
    }
  }

  for (auto& msg : msgs_to_be_handled) {
    int forwarded_key = -1;
    int forwarded_version = -1;
    if (HasConflict(third_party::SArray<Key>(msg.data[0]), min_clock, msg.meta.version - staleness_ - 1,
           &forwarded_key, &forwarded_version)) {
      main_recorder_[forwarded_version % main_recorder_version_level_size_][forwarded_key - range_.begin()].second.push_back(std::move(msg));
#ifdef USE_TIMER
      total_forward_ += 1;
#endif
    } else {
      msgs->push_back(std::move(msg));
    }
  }
}

void VectorSparseSSPRecorder::RemoveRecord(const int version) { 
  // for (int i = 0; i < main_recorder_[version % main_recorder_version_level_size_].size(); ++ i) {
  //   CHECK_EQ(main_recorder_[version % main_recorder_version_level_size_][i].first, 0);
  // }
}

/* IF:
 *   NO conflict: return 
 *   ONE or SEVERAL conflict: append to the corresponding get buffer, return true
 */
bool VectorSparseSSPRecorder::HasConflict(const third_party::SArray<Key>& keys, const int begin_version,
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
