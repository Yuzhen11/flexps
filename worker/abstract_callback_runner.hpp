#pragma once

#include <functional>

#include "base/message.hpp"

namespace flexps {

class AbstractCallbackRunner {
 public:
  virtual ~AbstractCallbackRunner() {}
  virtual void RegisterRecvHandle(uint32_t app_thread_id, uint32_t model_id, const std::function<void(Message&)>& recv_handle) = 0;
  virtual void RegisterRecvFinishHandle(uint32_t app_thread_id, uint32_t model_id, const std::function<void()>& recv_finish_handle) = 0;

  virtual void NewRequest(uint32_t app_thread_id, uint32_t model_id, uint32_t expected_responses) = 0;
  virtual void WaitRequest(uint32_t app_thread_id, uint32_t model_id) = 0;
};

}  // namespace flexps
