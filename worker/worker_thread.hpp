#pragma once

#include "base/message.hpp"
#include "base/threadsafe_queue.hpp"

#include <condition_variable>
#include <memory>
#include <thread>
#include <unordered_map>

namespace flexps {

class WorkerThread {
 public:
  WorkerThread(uint32_t worker_id) : worker_id_(worker_id) {}

  void RegisterThread(uint32_t model_id, );
  void Start();
  void Stop();
  ThreadsafeQueue<Message>* GetWorkQueue();
 private:
  uint32_t worker_id_;
  std::thread work_thread_;
  ThreadsafeQueue<Message> work_queue_;

  AppBlocker app_blocker_;
};

}  // namespace flexps
