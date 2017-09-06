#include "server/server_thread.hpp"

#include "base/magic.hpp"

#include "glog/logging.h"

namespace flexps {

void ServerThread::Start() {
  work_thread_ = std::thread([this] { Main(); });
}
void ServerThread::Stop() { work_thread_.join(); }

void ServerThread::RegisterModel(uint32_t model_id, std::unique_ptr<AbstractModel>&& model) {
  CHECK(models_.find(model_id) == models_.end());
  models_.insert(std::make_pair(model_id, std::move(model)));
}

ThreadsafeQueue<Message>* ServerThread::GetWorkQueue() { return &work_queue_; }

AbstractModel* ServerThread::GetModel(uint32_t model_id) {
  CHECK(models_.find(model_id) != models_.end());
  return models_[model_id].get();
}

void ServerThread::Main() {
  while (true) {
    Message msg;
    work_queue_.WaitAndPop(&msg);

    char flag;
    msg.bin >> flag;
    if (flag == kExit)
      break;

    uint32_t tid;
    uint32_t model_id;
    msg.bin >> tid >> model_id;
    CHECK(models_.find(model_id) != models_.end());
    if (flag == kClock) {
      models_[model_id]->Clock(tid);
    } else if (flag == kAdd) {
      models_[model_id]->Add(tid, msg);
    } else if (flag == kGet) {
      models_[model_id]->Get(tid, msg);
    }
  }
}

}  // namespace flexps
