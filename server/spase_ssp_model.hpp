s#include "server/abstract_model.hpp"

#include "base/message.hpp"
#include "base/threadsafe_queue.hpp"
#include "server/abstract_storage.hpp"
#include "server/pending_buffer.hpp"
#include "server/progress_tracker.hpp"
#include "server/storage.hpp"
#include "server/add_record_buffer.hpp"

#include <map>
#include <vector>

namespace flexps {

class SparseSSPModel : public AbstractModel {
 public:
  explicit SparseSSPModel(uint32_t model_id, std::unique_ptr<AbstractStorage>&& storage_ptr,
    ThreadsafeQueue<Message>* reply_queue, int staleness, int speculation);

  virtual void Clock(Message& message) override;
  virtual void Add(Message& message) override;
  virtual void Get(Message& message) override;
  virtual int GetProgress(int tid) override;
  virtual void ResetWorker(const std::vector<uint32_t> tids) override;

  void InitGet(Message& message);

  int GetGetPendingSize(int progress);
  int GetAddPendingSize(int progress);

 private:
  uint32_t model_id_;
  uint32_t staleness_;
  uint32_t speculation_;

  ThreadsafeQueue<Message>* reply_queue_;
  std::unique_ptr<AbstractStorage> storage_;
  ProgressTracker progress_tracker_;
  PendingBuffer get_buffer_;
};

}  // namespace flexps
