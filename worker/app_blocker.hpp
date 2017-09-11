#pragma once

#include <map>
#include <vector>
#include <condition_variable>
#include <mutex>

#include "base/message.hpp"
#include "worker/abstract_callback_runner.hpp"
#include "worker/abstract_receiver.hpp"

namespace flexps {

/*
 * Thread-safe.
 *
 * App threads block on AppBlocker by WaitRequest().
 * WorkerThread calls AddResponse() and run the callbacks.
 * May consider let other threads run the callbacks if there's performance issue (If worker thread is overloaded).
 *
 * Should register handle before use.
 * Should call NewRequest before sending out the request.
 */
class AppBlocker : public AbstractCallbackRunner, public AbstractReceiver {
 public:
  virtual void RegisterRecvHandle(uint32_t app_thread_id, uint32_t model_id, const std::function<void(Message&)>& recv_handle) override;
  virtual void RegisterRecvFinishHandle(uint32_t app_thread_id, uint32_t model_id, const std::function<void()>& recv_finish_handle) override;

  virtual void NewRequest(uint32_t app_thread_id, uint32_t model_id, uint32_t expected_responses) override;
  virtual void WaitRequest(uint32_t app_thread_id, uint32_t model_id) override;

  virtual void AddResponse(uint32_t app_thread_id, uint32_t model_id, Message& msg) override;
 private:
  void SanityCheck(uint32_t app_thread_id, uint32_t model_id); 

  std::condition_variable cond_;
  std::mutex mu_;

  // app_thread_id, model_id, <expected, current>
  std::map<uint32_t, std::map<uint32_t, std::pair<uint32_t, uint32_t>>> tracker_;

  // app_thread_id, model_id, callback
  std::map<uint32_t, std::map<uint32_t, std::function<void(Message& message)>>> recv_handle_;
  std::map<uint32_t, std::map<uint32_t, std::function<void()>>> recv_finish_handle_;
};

}  // namespace flexps
