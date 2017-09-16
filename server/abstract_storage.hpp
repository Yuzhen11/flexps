#pragma once

#include "base/message.hpp"

namespace flexps {

class AbstractStorage {
 public:
  virtual void Add(Message& msg) = 0;
  virtual Message Get(Message& msg) = 0;
  virtual void FinishIter() = 0;

 private:
  virtual void FindAndCreate(Key key) = 0;
};

}  // namespace flexps
