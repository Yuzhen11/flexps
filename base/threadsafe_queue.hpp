#pragma once

#include <condition_variable>
#include <memory>
#include <mutex>
#include <queue>

namespace flexps {

template <typename T>
class ThreadsafeQueue {
 public:
  ThreadsafeQueue() = default;
  ~ThreadsafeQueue() = default;
  ThreadsafeQueue(const ThreadsafeQueue&) = delete;
  ThreadsafeQueue& operator=(const ThreadsafeQueue&) = delete;
  ThreadsafeQueue(ThreadsafeQueue&&) = delete;
  ThreadsafeQueue& operator=(ThreadsafeQueue&&) = delete;

  void Push(T elem) {
    mu_.lock();
    queue_.push(std::move(elem));
    mu_.unlock();
    cond_.notify_all();
  }

  void WaitAndPop(T* elem) {
    std::unique_lock<std::mutex> lk(mu_);
    cond_.wait(lk, [this] { return !queue_.empty(); });
    *elem = std::move(queue_.front());
    queue_.pop();
  }

  int size() {
    std::lock_guard<std::mutex> lk(mu_);
    return queue_.size();
  }

 private:
  std::mutex mu_;
  std::queue<T> queue_;
  std::condition_variable cond_;
};

}  // namespace flexps
