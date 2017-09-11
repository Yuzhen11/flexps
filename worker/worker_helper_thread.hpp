#pragma once

#include "base/message.hpp"
#include "base/threadsafe_queue.hpp"
#include "worker/abstract_receiver.hpp"

#include <condition_variable>
#include <memory>
#include <thread>
#include <unordered_map>

namespace flexps {

class WorkerHelperThread {
 public:
  WorkerHelperThread(uint32_t helper_id, AbstractReceiver* const receiver) 
    : helper_id_(helper_id), receiver_(receiver) {}

  void Start();
  void Stop();
  ThreadsafeQueue<Message>* GetWorkQueue();
  uint32_t GetHelperId() const;
 private:
  void Main();

  uint32_t helper_id_;
  std::thread work_thread_;
  ThreadsafeQueue<Message> work_queue_;

  AbstractReceiver* const receiver_;
};

}  // namespace flexps
