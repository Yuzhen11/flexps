#pragma once

#include "base/threadsafe_queue.hpp"
#include "server/abstract_model.hpp"
#include "server/message.hpp"

#include <thread>
#include <unordered_map>

namespace flexps {

class ServerThread {
 public:
  ServerThread() = default;
  ~ServerThread() = default;

  void RegisterModel(uint32_t model_id, std::unique_ptr<AbstractModel>&& model);
  void Start();
  void Stop();
  ThreadsafeQueue<Message>* GetWorkQueue();

  void Main();

  AbstractModel* GetModel(uint32_t model_id);

 private:
  std::thread work_thread_;
  ThreadsafeQueue<Message> work_queue_;
  std::unordered_map<uint32_t, std::unique_ptr<AbstractModel>> models_;
};

}  // namespace flexps
