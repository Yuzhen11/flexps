#pragma once

#include <cinttypes>
#include "server/message.hpp"

namespace flexps {

class AbstractModel {
 public:
  virtual void Clock(uint32_t tid) = 0;
  virtual void Add(uint32_t tid, const Message& message) = 0;
  virtual void Get(uint32_t tid, const Message& message) = 0;
  virtual ~AbstractModel() {}
};

}  // namespace flexps
