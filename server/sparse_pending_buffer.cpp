#include "server/sparse_pending_buffer.hpp"
#include "glog/logging.h"

namespace flexps {

std::vector<Message> SparsePendingBuffer::Pop(const int version, const int tid) {
  std::vector<Message> poped_message;
  if (buffer_.find(version) != buffer_.end()) {
    if (buffer_[version].find(tid) != buffer_[version].end()) {
      poped_message = std::move(buffer_[version][tid]);
      buffer_[version].erase(tid);
      if (buffer_[version].size() == 0)
        buffer_.erase(version);
    }
  }
  return poped_message;
}

void SparsePendingBuffer::Push(const int version, Message& message, const int tid) {
  buffer_[version][tid].push_back(std::move(message));
}

int SparsePendingBuffer::Size(const int version) {
  if (buffer_.find(version) == buffer_.end()) {
    return 0;
  }
  int size = 0;
  for (auto& tid_messages : buffer_[version]) {
    size += tid_messages.second.size();
  }
  return size;
}

}  // namespace flexps
