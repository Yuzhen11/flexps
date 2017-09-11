#pragma once

#include "base/message.hpp"

namespace flexps {

class AbstractReceiver {
 public:
  virtual ~AbstractReceiver() {}
  virtual void AddResponse(uint32_t app_thread_id, uint32_t model_id, Message& msg) = 0;
};

}  // namespace flexps
