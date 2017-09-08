#include "worker/worker_thread.hpp"

#include "glog/logging.h"

namespace flexps {

void WorkerThread::Start() {
  work_thread_ = std::thread([this] { Main(); });
}

void WorkerThread::Stop() { work_thread_.join(); }

ThreadsafeQueue<Message>* WorkerThread::GetWorkQueue() { return &work_queue_; }

uint32_t WorkerThread::GetWorkerId() const { return worker_id_; }

void WorkerThread::Main() {
  while (true) {
    Message msg;
    work_queue_.WaitAndPop(&msg);
  }
}

}  // namespace flexps
