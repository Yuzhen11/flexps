#pragma once

#include "base/message.hpp"
#include "base/threadsafe_queue.hpp"
#include "server/abstract_model.hpp"

#include <thread>
#include <unordered_map>
#ifdef USE_TIMER
#include <chrono>
#endif

#include "glog/logging.h"

namespace flexps {

class ServerThread {
 public:
  ServerThread(uint32_t server_id) : server_id_(server_id) {}
  ~ServerThread();

  void RegisterModel(uint32_t model_id, std::unique_ptr<AbstractModel>&& model);
  void Start();
  void Stop();
  ThreadsafeQueue<Message>* GetWorkQueue();

  void Main();

  AbstractModel* GetModel(uint32_t model_id);
  uint32_t GetServerId() const;

 private:
  uint32_t server_id_;
  std::thread work_thread_;
  ThreadsafeQueue<Message> work_queue_;
  std::unordered_map<uint32_t, std::unique_ptr<AbstractModel>> models_;

#ifdef USE_TIMER
  std::chrono::microseconds clock_time_{0};
  std::chrono::microseconds add_time_{0};
  std::chrono::microseconds get_time_{0};
#endif
};

}  // namespace flexps
