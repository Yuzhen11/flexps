#include "server/pending_buffer.hpp"

namespace flexps {

std::vector<Message> PendingBuffer::Pop(const int clock, const int tid) {
  std::vector<Message> poped_msg;
  if (buffer_.find(clock) != buffer_.end()) {
    poped_msg = std::move(buffer_[clock]);
    buffer_.erase(clock);
  }
  return poped_msg;
}

void PendingBuffer::Push(const int clock, Message& msg, const int tid) {
  // Todo(Ruoyu Wu): check the clock passed in is less than the minimum clock in the buffer
  buffer_[clock].push_back(std::move(msg)); 
}

int PendingBuffer::Size(const int progress) { return buffer_[progress].size(); }

}  // namespace flexps

