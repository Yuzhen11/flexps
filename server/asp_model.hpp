#pragma once

#include "server/abstract_model.hpp"

#include "base/message.hpp"
#include "base/threadsafe_queue.hpp"
#include "server/abstract_storage.hpp"
#include "server/pending_buffer.hpp"
#include "server/progress_tracker.hpp"


namespace flexps {

class ASPModel : public AbstractModel {
 public:
  explicit ASPModel(uint32_t model_id, std::unique_ptr<AbstractStorage>&& storage_ptr,
                    ThreadsafeQueue<Message>* reply_queue);

  virtual void Clock(Message& msg) override;
  virtual void Add(Message& msg) override;
  virtual void Get(Message& msg) override;
  virtual int GetProgress(int tid) override;
  virtual void ResetWorker(Message& msg) override;

 private:
  uint32_t model_id_;

  ThreadsafeQueue<Message>* reply_queue_;
  std::unique_ptr<AbstractStorage> storage_;
  ProgressTracker progress_tracker_;
};

}  // namespace flexps
