#pragma once

#include "server/abstract_model.hpp"

#include "base/message.hpp"
#include "base/threadsafe_queue.hpp"
#include "server/abstract_storage.hpp"
#include "server/progress_tracker.hpp"
#include "server/map_storage.hpp"

#include "server/abstract_sparse_ssp_recorder_2.hpp"
#include "server/unordered_map_sparse_ssp_recorder.hpp"
#include "server/vector_sparse_ssp_recorder.hpp"

#include <map>
#include <vector>

namespace flexps {

class SparseSSPModel2 : public AbstractModel {
 public:
  explicit SparseSSPModel2(const uint32_t model_id, std::unique_ptr<AbstractStorage>&& storage, 
                          std::unique_ptr<AbstractSparseSSPRecorder2> recorder,
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

  std::unique_ptr<AbstractSparseSSPRecorder2> recorder_;

  std::vector<Message> buffer_;
  int staleness_;
  int speculation_;
};

}  // namespace flexps
