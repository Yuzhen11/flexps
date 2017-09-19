#pragma once

#include "base/message.hpp"

#include <vector>

namespace flexps {

class AbstractSparseSSPController {
 public:
  virtual ~AbstractSparseSSPController() {}
  virtual std::vector<Message> UnblockRequests(int progress, int sender, int updated_min_clock, int min_clock) = 0;
  virtual void AddRecord(Message& msg) = 0;
};


}  // namespace flexps

