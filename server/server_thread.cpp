#include "server/server_thread.hpp"

#include "glog/logging.h"

namespace flexps {

ServerThread::~ServerThread() {
#ifdef USE_TIMER
  LOG(INFO) << "clock_time: " << clock_time_.count()/1000. << " ms";
  LOG(INFO) << "add_time: " << add_time_.count()/1000. << " ms";
  LOG(INFO) << "get_time: " << get_time_.count()/1000. << " ms";
#endif
}

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
    case Flag::kClock: {
#ifdef USE_TIMER
      auto start_time = std::chrono::steady_clock::now();
#endif
      models_[model_id]->Clock(msg);
#ifdef USE_TIMER
      auto end_time = std::chrono::steady_clock::now();
      clock_time_ += std::chrono::duration_cast<std::chrono::microseconds>(end_time - start_time);
#endif
      break;
    }
    case Flag::kAdd: {
#ifdef USE_TIMER
      auto start_time = std::chrono::steady_clock::now();
#endif
      models_[model_id]->Add(msg);
#ifdef USE_TIMER
      auto end_time = std::chrono::steady_clock::now();
      add_time_ += std::chrono::duration_cast<std::chrono::microseconds>(end_time - start_time);
#endif
      break;
    }
    case Flag::kGet: {
#ifdef USE_TIMER
      auto start_time = std::chrono::steady_clock::now();
#endif
      models_[model_id]->Get(msg, false);
#ifdef USE_TIMER
      auto end_time = std::chrono::steady_clock::now();
      get_time_ += std::chrono::duration_cast<std::chrono::microseconds>(end_time - start_time);
#endif
      break;
    }
    case Flag::kGetChunk: {
#ifdef USE_TIMER
      auto start_time = std::chrono::steady_clock::now();
#endif
      models_[model_id]->Get(msg, true);
#ifdef USE_TIMER
      auto end_time = std::chrono::steady_clock::now();
      get_time_ += std::chrono::duration_cast<std::chrono::microseconds>(end_time - start_time);
#endif
      break;
    }
    case Flag::kResetWorkerInModel: {
      models_[model_id]->ResetWorker(msg);

      break;
    }
    default:
      CHECK(false) << "Unknown flag in msg: " << FlagName[static_cast<int>(msg.meta.flag)];
    }
  }
}

}  // namespace flexps
