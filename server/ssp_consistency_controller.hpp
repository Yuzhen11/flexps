#include "server/abstract_model.hpp"

#include "server/abstract_storage.hpp"
#include "server/message.hpp"
#include "server/pending_buffer.hpp"
#include "server/progress_tracker.hpp"

#include <map>

namespace flexps {

class SSPConsistencyController : public AbstractModel {
 public:
  SSPConsistencyController(int staleness) : staleness_(staleness) {}

  virtual void Clock(uint32_t tid) override;
  virtual void Add(uint32_t tid, const Message& message) override;
  virtual void Get(uint32_t tid, const Message& message) override;

 private:
  int staleness_;

  std::unique_ptr<AbstractStorage> storage_;
  ProgressTracker progress_tracker_;
  PendingBuffer buffer_;
};

}  // namespace flexps
