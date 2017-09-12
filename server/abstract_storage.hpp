#pragma once

#include "base/message.hpp"

namespace flexps {

class AbstractStorage {
 public:
  virtual void Add(Message& message) = 0;
  virtual Message Get(Message& message) = 0;
  virtual void FinishIter() = 0;

 private:
  virtual void FindAndCreate(int key) = 0;
};

}  // namespace flexps
