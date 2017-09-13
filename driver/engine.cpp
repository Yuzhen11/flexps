#include "driver/engine.hpp"

#include <thread>
#include <vector>

#include "glog/logging.h"

namespace flexps {

void Engine::CreateMailbox() {
  mailbox_.reset(new Mailbox(node_));
}

void Engine::StartWorkerHelperThreads() {
  app_blocker_.reset(new AppBlocker());
  worker_helper_thread_.reset(new WorkerHelperThread(..., app_blocker_.get()));  // worker helper id
  mailbox_->RegisterQueue(worker_helper_thread_.GetHelperId(), worker_helper_thread_.GetWorkQueue());
  worker_helper_thread_.Start();
  VLOG(1) << "worker_helper_thread starts on node" << node_.id;
}

void Engine::StartServerThreads() {
  server_thread_group_.reset(new ServerThreadGroup({...}));  // server thread id
  for (auto& server_thread : *server_thread_group_) {
    mailbox_.RegisterQueue(server_thread.GetServerId(), server_thread.GetWorkQueue());
    server_thread.Start();
  }
  VLOG(1) << "server_threads start on node" << node_.id;
}

void Engine::StartMailbox() {
  LOG(INFO) << mailbox_->GetQueueMapSize() << " threads are registered to node" << node_.id;
  mailbox_->Start(nodes_);
  VLOG(1) << "mailbox starts on node" << node_.id;
}


void Engine::StopWorkerHelperThreads() {
  worker_helper_thread_.Stop();
  VLOG(1) << "worker_helper_thread stops on node" << node_.id;
}

void Engine::StopServerThreads() {
  for (server_thread : *server_thread_group_) {
    server_thread.Stop();
  }
  VLOG(1) << "server_threads stop on node" << node_.id;
}

void Engine::StopMailbox() {
  mailbox_->Stop();
  VLOG(1) << "mailbox stops on node" << node_.id;
}

void Engine::CreateTable(uint32_t table_id, const SimpleRangeManager& range_manager) {
  CHECK(range_manager_map_.find(table_id) == range_manager_map_.end());
  range_manager_map_.insert({table_id, range_manager});
  for (auto& server_thread : *server_thread_group_) {
    std::unique_ptr<AbstractStorage> storage(new Storage<int>());
    // TODO: model_id, tids
    std::unique_ptr<AbstractModel> model(new SSPModel(model_id, tids, std::move(storage), model_staleness,
                                                      server_thread_group.GetReplyQueue()));
    server_thread->RegisterModel(i, std::move(model));
  }
}

void Engine::Run(const MLTask& task) {
  const auto& worker_spec = task.GetWorkerSpec();
  if (worker_spec.HasLocalWorkers(proc_id_)) {
    const auto& local_workers = worker_spec.GetLocalWorkers(proc_id_);
    std::vector<std::thread> thread_group(local_workers.size());
    for (int i = 0; i < thread_group.size(); ++ i) {
      LOG(INFO) << thread_group.size() << " workers run on proc: " << proc_id_;
      thread_group[i] = std::thread([&task](){
        Info info;
        task.RunLambda(info);
      });
    }
    for (auto& th : thread_group) {
      th.join();
    }
  }
}

}  // namespace flexps
