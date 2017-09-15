#include "server/server_thread.hpp"

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

uint32_t ServerThread::GetServerId() const { return server_id_; }

AbstractModel* ServerThread::GetModel(uint32_t model_id) {
  CHECK(models_.find(model_id) != models_.end());
  return models_[model_id].get();
}

void ServerThread::Main() {
  while (true) {
    Message msg;
    work_queue_.WaitAndPop(&msg);

    if (msg.meta.flag == Flag::kExit)
      break;

    uint32_t model_id = msg.meta.model_id;
    CHECK(models_.find(model_id) != models_.end()) << "Unknown model_id: " << model_id;
    switch (msg.meta.flag) {
    case Flag::kClock:
      models_[model_id]->Clock(msg);
      break;
    case Flag::kAdd:
      models_[model_id]->Add(msg);
      break;
    case Flag::kGet:
      models_[model_id]->Get(msg);
      break;
    case Flag::kResetWorkerInModel: {
      CHECK_EQ(msg.data.size(), 1);
      third_party::SArray<uint32_t> tids;
      tids = msg.data[0];
      std::vector<uint32_t> tids_vec;
      for (auto tid : tids) tids_vec.push_back(tid);
      models_[model_id]->ResetWorker(tids_vec);
      break;
    }
    default:
      CHECK(false) << "Unknown flag in message: " << FlagName[static_cast<int>(msg.meta.flag)];
    }
  }
}

}  // namespace flexps
