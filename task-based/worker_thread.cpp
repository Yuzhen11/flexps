#include "task-based/scheduler_thread.hpp"
#include "glog/logging.h"

namespace flexps {

WorkerThread::~WorkerThread() {
  mailbox_.DeregisterQueue(worker_id_);
}

void WorkerThread::Start() {
  work_thread_ = std::thread([this] { Main(); });
}
void WorkerThread::Stop() {
  work_thread_.join();
}

ThreadsafeQueue<Message>* WorkerThread::GetWorkQueue() { return &work_queue_; }

uint32_t WorkerThread::GetWorkerId() const { return worker_id_; }

void WorkerThread::Main() {
  while (true) {
    Message msg;
    work_queue_.WaitAndPop(&msg);
    CHECK(msg.meta.recver == worker_id_);
    LOG(INFO) << msg.DebugString();

    if (msg.meta.flag == Flag::kExit)
      break;
  }
}

}  // namespace flexps
