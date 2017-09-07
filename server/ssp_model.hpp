#include "server/abstract_model.hpp"

#include "server/abstract_storage.hpp"
#include "base/message.hpp"
#include "server/pending_buffer.hpp"
#include "server/progress_tracker.hpp"

#include <map>

namespace flexps {

class SSPModel : public AbstractModel {
 public:
  SSPModel(int staleness) : staleness_(staleness) {}

  virtual void Clock(Message& message) override;
  virtual void Add(Message& message) override;
  virtual void Get(Message& message) override;

 private:
  int staleness_;

  std::unique_ptr<AbstractStorage> storage_;
  ProgressTracker progress_tracker_;
  PendingBuffer buffer_;
};

}  // namespace flexps
