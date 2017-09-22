#pragma once

#include "server/abstract_model.hpp"

#include "base/message.hpp"
#include "base/threadsafe_queue.hpp"
#include "server/abstract_storage.hpp"
#include "server/progress_tracker.hpp"
#include "server/map_storage.hpp"
#include "server/sparse_ssp_controller.hpp"

#include <map>
#include <vector>

namespace flexps {

class SparseSSPModel : public AbstractModel {
 public:
  explicit SparseSSPModel(const uint32_t model_id, std::unique_ptr<AbstractStorage>&& storage,
                          ThreadsafeQueue<Message>* reply_queue, int staleness, int speculation);

  virtual void Clock(Message& message) override;
  virtual void Add(Message& message) override;
  virtual void Get(Message& message) override;
  virtual int GetProgress(int tid) override;
  virtual void ResetWorker(Message& msg) override;

 private:
  uint32_t model_id_;

  ThreadsafeQueue<Message>* reply_queue_;
  std::unique_ptr<AbstractStorage> storage_;
  ProgressTracker progress_tracker_;

  SparseSSPController sparse_ssp_controller_;
};

}  // namespace flexps
