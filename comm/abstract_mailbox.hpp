#pragma once

#include "base/message.hpp"
#include "base/node.hpp"

namespace flexps {

class AbstractMailbox {
 public:
  virtual int Send(const Message& msg) = 0;
  virtual int Recv(Message* msg) = 0;
  virtual void Start() = 0;
  virtual void Stop() = 0;

 private:
  virtual void Connect(const Node& node) = 0;
  virtual void Bind(const Node& node) = 0;

  virtual void Receiving() = 0;
};

}  // namespace flexps
