#include "worker/model_init_thread.hpp"

#include "glog/logging.h"

namespace flexps {

void ModelInitThread::Start() {
  work_thread_ = std::thread([this] { Main(); });
}

void ModelInitThread::Stop() { work_thread_.join(); }

ThreadsafeQueue<Message>* ModelInitThread::GetWorkQueue() {
  return &work_queue_;
}

uint32_t ModelInitThread::GetThreadId() const { return thread_id_; }

void ModelInitThread::ResetWorkerInModel(uint32_t model_id, const std::vector<uint32_t>& local_servers,
    const std::vector<uint32_t>& worker_ids) {
  {
    std::lock_guard<std::mutex> lk(mu_);
    CHECK(local_servers_count_.find(model_id) == local_servers_count_.end());
    local_servers_count_.insert({model_id, {local_servers.size(), 0}});
  }
  for (auto local_server : local_servers) {
    Message msg;
    msg.meta.recver = local_server;
    msg.meta.model_id = model_id;
    msg.meta.flag = Flag::kResetWorkerInModel;
    third_party::SArray<uint32_t> wids(worker_ids);
    msg.AddData(wids);
    downstream_->Push(msg);
  }
  {
    std::unique_lock<std::mutex> lk(mu_);
    cond_.wait(lk, [&] { return local_servers_count_[model_id].first == local_servers_count_[model_id].second; });
    local_servers_count_.erase(model_id);
  }
}

void ModelInitThread::Main() {
  while (true) {
    Message msg;
    work_queue_.WaitAndPop(&msg);

    if (msg.meta.flag == Flag::kExit) {
      break;
    } else if (msg.meta.flag == Flag::kResetWorkerInModel) {
      std::unique_lock<std::mutex> lk(mu_);
      uint32_t model_id = msg.meta.model_id;
      CHECK(local_servers_count_.find(model_id) != local_servers_count_.end());
      local_servers_count_[model_id].second += 1;
      if (local_servers_count_[model_id].first == local_servers_count_[model_id].second) {
        cond_.notify_all();
      }
    } else {
      CHECK(false) << "Unknown flag in message: " << FlagName[static_cast<int>(msg.meta.flag)];
    }
  }
}

}  // namespace flexps

