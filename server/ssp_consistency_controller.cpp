#include "server/ssp_consistency_controller.hpp"
#include "glog/logging.h"

namespace flexps {

void SSPConsistencyController::Clock(int tid) {
  int updated_min_clock = progress_tracker_.AdvanceAndGetChangedMinClock(tid);
  if (updated_min_clock != -1) {  // min clock updated
    auto reqs_blocked_at_this_min_clock = buffer_.Pop(updated_min_clock);
    for (auto req : reqs_blocked_at_this_min_clock) {
      storage_->Apply(req);
    }
    storage_->FinishIter();
  }
}

void SSPConsistencyController::Add(int tid, const Message& message) {
  // The add will always never be blocked
  storage_->Apply(message);
}

void SSPConsistencyController::Get(int tid, const Message& message) {
  CHECK(progress_tracker_.CheckThreadValid(tid));
  int progress = progress_tracker_.GetProgress(tid);
  int min_clock = progress_tracker_.GetMinClock();
  if (progress - min_clock > staleness_) {
    buffer_.Push(progress - staleness_, message);
  } else {
    storage_->Apply(message);
  }
}

}  // namespace flexps
