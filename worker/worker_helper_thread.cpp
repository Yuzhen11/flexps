#include "worker/worker_helper_thread.hpp"

#include "glog/logging.h"

namespace flexps {

void WorkerHelperThread::Start() {
  work_thread_ = std::thread([this] { Main(); });
}

void WorkerHelperThread::Stop() { work_thread_.join(); }

ThreadsafeQueue<Message>* WorkerHelperThread::GetWorkQueue() { return &work_queue_; }

uint32_t WorkerHelperThread::GetHelperId() const { return helper_id_; }

void WorkerHelperThread::Main() {
  while (true) {
    Message msg;
    work_queue_.WaitAndPop(&msg);

    if (msg.meta.flag == Flag::kExit)
      break;

    CHECK_NOTNULL(receiver_);
    receiver_->AddResponse(msg.meta.recver, msg.meta.model_id, msg);
  }
}

}  // namespace flexps
