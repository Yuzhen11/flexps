#include "server/abstract_model.hpp"

#include "base/message.hpp"
#include "base/threadsafe_queue.hpp"
#include "server/abstract_storage.hpp"
#include "server/pending_buffer.hpp"
#include "server/progress_tracker.hpp"
#include "server/storage.hpp"

#include <map>
#include <vector>

namespace flexps {

class SSPModel : public AbstractModel {
 public:
  explicit SSPModel(uint32_t model_id, std::unique_ptr<AbstractStorage>&& storage_ptr,
                    int staleness, ThreadsafeQueue<Message>* reply_queue);

  virtual void Clock(Message& message) override;
  virtual void Add(Message& message) override;
  virtual void Get(Message& message) override;
  virtual int GetProgress(int tid) override;
  virtual void ResetWorker(std::vector<int> tids) override;

  int GetPendingSize(int progress);

 private:
  uint32_t model_id_;
  uint32_t staleness_;

  ThreadsafeQueue<Message>* reply_queue_;
  std::unique_ptr<AbstractStorage> storage_;
  ProgressTracker progress_tracker_;
  PendingBuffer buffer_;
};

}  // namespace flexps
