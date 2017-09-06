#include "server/ssp_consistency_controller.hpp"
#include "glog/logging.h"

namespace flexps {

void SSPConsistencyController::Clock(uint32_t tid) {
  int updated_min_clock = progress_tracker_.AdvanceAndGetChangedMinClock(tid);
  if (updated_min_clock != -1) {  // min clock updated
    // auto reqs_blocked_at_this_min_clock = buffer_.Pop(updated_min_clock);
    // for (auto req : reqs_blocked_at_this_min_clock) {
    //   storage_->Get(req);
    // }
    // storage_->FinishIter();
  }
}

void SSPConsistencyController::Add(uint32_t tid, const Message& message) {
  // The add will always never be blocked
  storage_->Add(message);
}

void SSPConsistencyController::Get(uint32_t tid, const Message& message) {
  CHECK(progress_tracker_.CheckThreadValid(tid));
  int progress = progress_tracker_.GetProgress(tid);
  int min_clock = progress_tracker_.GetMinClock();
  if (progress - min_clock > staleness_) {
    // buffer_.Push(progress - staleness_, message);
  } else {
    storage_->Get(message);
  }
}

}  // namespace flexps
