#include "server/abstract_model.hpp"

#include "base/message.hpp"
#include "server/abstract_storage.hpp"
#include "server/pending_buffer.hpp"
#include "server/progress_tracker.hpp"

#include <map>

namespace flexps {

class SSPModel : public AbstractModel {
 public:
  explicit SSPModel(uint32_t model_id) : model_id_(model_id) {}

  // Initialize staleness, progress_tracker_, storage_
  void Init();

  virtual void Clock(Message& message) override;
  virtual void Add(Message& message) override;
  virtual void Get(Message& message) override;

 private:
  uint32_t model_id_;
  uint32_t staleness_;

  std::unique_ptr<AbstractStorage> storage_;
  ProgressTracker progress_tracker_;
  PendingBuffer buffer_;
};

}  // namespace flexps
