#pragma once

#include "base/message.hpp"

#include <unordered_map>
#include <vector>

namespace flexps {

class PendingBuffer {
 public:
  std::vector<Message> Pop(int clock);
  void Push(int clock, Message& msg);
  int Size(int progress);

 private:
  // TODO(Ruoyu Wu): each vec of buffer_ should be pop one by one, now it is not guaranteed
  std::unordered_map<int, std::vector<Message>> buffer_;
};

}  // namespace flexps
