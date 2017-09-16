#include "server/sparse_pending_buffer.hpp"

namespace flexps {

std::vector<Message> PendingBuffer::Pop(int clock, int tid) {
  std::vector<Message> poped_message;
  if (buffer_.find(clock) != buffer_.end()) {
    poped_message = std::move(buffer_[clock]);
    buffer_.erase(clock);
  }
  return poped_message;
}

void PendingBuffer::Push(int clock, Message& message, int tid) {
  // Todo(Ruoyu Wu): check the clock passed in is less than the minimum clock in the buffer
  if (buffer_.find(clock) == buffer_.end()) {
    buffer_[clock] = std::vector<Message>({std::move(message)});
  } else {
    buffer_[clock].push_back(std::move(message));
  }
}

int PendingBuffer::Size(int progress) { 
  CHECK(buffer_.find(progress) != buffer_.end()) << "[ERROR]sparse_pending_buffer: progress out of index";
  int size = 0;
  for (auto& tid_messages_in_this_progress : buffer_) {
    size += tid_messages_in_this_progress.size();
  }
  return size;
}

}  // namespace flexps