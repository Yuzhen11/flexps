#include "server/progress_tracker.hpp"

#include "glog/logging.h"

namespace flexps {

void ProgressTracker::Init(const std::vector<uint32_t>& tids) {
  progresses_.clear();
  for (auto tid : tids) {
    progresses_.insert({tid, 0});
  }
  min_clock_ = 0;
}

int ProgressTracker::AdvanceAndGetChangedMinClock(int tid) {
  CHECK(CheckThreadValid(tid));
  if (IsUniqueMin(tid)) {
    min_clock_ += 1;
    progresses_[tid] += 1;
    return min_clock_;
  } else {
    progresses_[tid] += 1;
    return -1;
  }
}

int ProgressTracker::GetNumThreads() const { return progresses_.size(); }

int ProgressTracker::GetProgress(int tid) const {
  CHECK(CheckThreadValid(tid));
  auto it = progresses_.find(tid);
  return it->second;
}

int ProgressTracker::GetMinClock() const { return min_clock_; }

bool ProgressTracker::IsUniqueMin(int tid) const {
  CHECK(CheckThreadValid(tid));
  auto it = progresses_.find(tid);
  if (it->second != min_clock_) {
    return false;
  }
  int min_count = 0;
  for (auto it : progresses_) {
    if (it.second == min_clock_)
      min_count += 1;
    if (min_count > 1)
      return false;
  }
  return true;
}

bool ProgressTracker::CheckThreadValid(int tid) const {
  auto it = progresses_.find(tid);
  return it != progresses_.end();
}

}  // namespace flexps
