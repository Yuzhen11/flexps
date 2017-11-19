#include "task-based/worker_thread.hpp"
#include "glog/logging.h"

namespace flexps {

WorkerThread::WorkerThread(uint32_t worker_id, AbstractMailbox* const mailbox) : worker_id_(worker_id), mailbox_(mailbox) {
  CHECK(worker_id != 0);
  mailbox_->RegisterQueue(worker_id, &work_queue_);
}

WorkerThread::~WorkerThread() {
  mailbox_->DeregisterQueue(worker_id_);
}

void WorkerThread::Start() {
  work_thread_ = std::thread([this] { Main(); });
}

void WorkerThread::Stop() {
  work_thread_.join();
}

void WorkerThread::Send(const Message& msg) {
  CHECK(msg.meta.recver == 0); // worker can only send message to scheduler for now
  mailbox_->Send(msg);
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
