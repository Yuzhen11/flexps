#pragma once

#include "base/message.hpp"
#include "base/node.hpp"

namespace flexps {

class AbstractMailbox {
 public:
  virtual ~AbstractMailbox() {}
  virtual int Send(const Message& msg) = 0;
};

}  // namespace flexps
