#include "task-based/scheduler_thread.hpp"
#include "glog/logging.h"

namespace flexps {

  SchedulerThread::SchedulerThread(AbstractMailbox* const mailbox) : mailbox_(mailbox) {
    mailbox_->RegisterQueue(scheduler_id_, &work_queue_);
  }

  SchedulerThread::~SchedulerThread() {
    mailbox_->DeregisterQueue(scheduler_id_);
  }

  void SchedulerThread::Start() {
    work_thread_ = std::thread([this] { Main(); });
  }

  void SchedulerThread::Stop() { 
    work_thread_.join();
  }

  void SchedulerThread::Send(const Message& msg) {
    mailbox_->Send(msg);
  }

  void SchedulerThread::RegisterWorker(uint32_t worker_id) {
    CHECK(worker_ids_.find(worker_id) == worker_ids_.end());
    worker_ids_.insert(worker_id);
  }

  ThreadsafeQueue<Message>* SchedulerThread::GetWorkQueue() { return &work_queue_; }

  std::set<uint32_t> SchedulerThread::GetWorkerIds() { return worker_ids_; }

  uint32_t SchedulerThread::GetSchedulerId() const { return scheduler_id_; }

  void SchedulerThread::Main() {
    while (true) {
      Message msg;
      work_queue_.WaitAndPop(&msg);
      CHECK(msg.meta.recver == scheduler_id_);
      LOG(INFO) << msg.DebugString();

      if (msg.meta.flag == Flag::kExit)
        break;
    }
  }

}  // namespace flexps
