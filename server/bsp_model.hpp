#include "server/abstract_model.hpp"

#include "base/message.hpp"
#include "base/threadsafe_queue.hpp"
#include "server/abstract_storage.hpp"
#include "server/pending_buffer.hpp"
#include "server/progress_tracker.hpp"

#include <map>
#include <vector>

namespace flexps {


/* 
 * TODO: The BSPModel is now problematic!!!
 * This is because the Get() request is only sent to the servers that have the keys, and thus
 * it is possible that a worker is fast so that a server may receive Clock() which is min_clock + 2.
 */
class BSPModel : public AbstractModel {
 public:
  explicit BSPModel(uint32_t model_id, std::unique_ptr<AbstractStorage>&& storage_ptr,
                    ThreadsafeQueue<Message>* reply_queue);

  virtual void Clock(Message& msg) override;
  virtual void Add(Message& msg) override;
  virtual void Get(Message& msg) override;
  virtual int GetProgress(int tid) override;
  virtual void ResetWorker(Message& msg) override;

  int GetGetPendingSize();
  int GetAddPendingSize();

 private:
  uint32_t model_id_;

  ThreadsafeQueue<Message>* reply_queue_;
  std::unique_ptr<AbstractStorage> storage_;
  ProgressTracker progress_tracker_;
  std::vector<Message> get_buffer_;
  std::vector<Message> add_buffer_;
};

}  // namespace flexps
