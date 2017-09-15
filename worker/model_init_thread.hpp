#pragma once

#include "base/message.hpp"
#include "base/threadsafe_queue.hpp"

#include <thread>
#include <cinttypes>
#include <mutex>
#include <condition_variable>

namespace flexps {

class ModelInitThread {
 public:
  ModelInitThread(uint32_t thread_id, ThreadsafeQueue<Message>* const downstream)
    : thread_id_(thread_id), downstream_(downstream) {}
  void Start();
  void Stop();
  uint32_t GetThreadId() const;
  ThreadsafeQueue<Message>* GetWorkQueue();
  void ResetWorkerInModel(uint32_t model_id, const std::vector<uint32_t>& local_servers,
      const std::vector<uint32_t>& worker_ids);
 private:
  void Main();

  uint32_t thread_id_;
  std::thread work_thread_;
  ThreadsafeQueue<Message> work_queue_;
  // Not owned
  ThreadsafeQueue<Message>* const downstream_;

  std::mutex mu_;
  std::condition_variable cond_;
  std::map<uint32_t, std::pair<uint32_t, uint32_t>> local_servers_count_;  // {model_id, {expected, current}}
  std::map<uint32_t, std::pair<uint32_t, uint32_t>> model_init_threads_count_;  // {model_id, {expected, current}}
};

}  // namespace flexps
