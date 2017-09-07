#pragma once

#include <cinttypes>
#include "base/message.hpp"

namespace flexps {

class AbstractModel {
 public:
  virtual void Clock(Message& message) = 0;
  virtual void Add(Message& message) = 0;
  virtual void Get(Message& message) = 0;
  virtual ~AbstractModel() {}
};

}  // namespace flexps
