#include "server/abstract_consistency_controller.hpp"

#include "server/message.hpp"
#include "server/pending_buffer.hpp"
#include "server/progress_tracker.hpp"

#include <map>

namespace flexps {

class SSPConsistencyController : public AbstractConsistencyController {
 public:
  SSPConsistencyController(int staleness, const ProgressTracker* const p_progress_tracker)
    :staleness_(staleness), p_progress_tracker_(p_progress_tracker) {}

  virtual void Clock(int tid) override;
  virtual void Add(int tid, const Message& message) override;
  virtual void Get(int tid, const Message& message) override;
 private:
  int staleness_;

  std::unique_ptr<AbstractStorage> storage_;
  ProgressTracker progress_tracker_;
  PendingBuffer buffer_;
};

}  // namespace flexps
