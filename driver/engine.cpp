#include "driver/engine.hpp"

#include "server/storage.hpp"
#include "server/ssp_model.hpp"

#include <thread>
#include <vector>

#include "glog/logging.h"

namespace flexps {

void Engine::CreateMailbox() {
  id_mapper_.reset(new SimpleIdMapper(node_, nodes_));
  const int kNumServerThreadPerNode = 1;
  id_mapper_->Init(kNumServerThreadPerNode);
  mailbox_.reset(new Mailbox(node_, nodes_, id_mapper_.get()));
}

void Engine::StartSender() {
  sender_.reset(new Sender(mailbox_.get()));
  sender_->Start();
}

void Engine::StartWorkerHelperThreads() {
  CHECK(id_mapper_);
  CHECK(mailbox_);
  auto worker_helper_thread_ids = id_mapper_->GetWorkerHelperThreadsForId(node_.id);
  CHECK_EQ(worker_helper_thread_ids.size(), 1);
  app_blocker_.reset(new AppBlocker());
  worker_helper_thread_.reset(new WorkerHelperThread(worker_helper_thread_ids[0], app_blocker_.get()));
  mailbox_->RegisterQueue(worker_helper_thread_->GetHelperId(), worker_helper_thread_->GetWorkQueue());
  worker_helper_thread_->Start();
  VLOG(1) << "worker_helper_thread:" << worker_helper_thread_ids[0] << " starts on node:" << node_.id;
}

void Engine::StartServerThreads() {
  CHECK(sender_);
  CHECK(mailbox_);
  auto server_thread_ids = id_mapper_->GetServerThreadsForId(node_.id);
  CHECK_GT(server_thread_ids.size(), 0);
  server_thread_group_.reset(new ServerThreadGroup(server_thread_ids, sender_->GetMessageQueue()));
  for (auto& server_thread : *server_thread_group_) {
    mailbox_->RegisterQueue(server_thread->GetServerId(), server_thread->GetWorkQueue());
    server_thread->Start();
  }
  std::stringstream ss;
  for (auto id : server_thread_ids) {
    ss << id << " ";
  }
  VLOG(1) << "server_threads:" << ss.str() << " start on node:" << node_.id;
}

void Engine::StartMailbox() {
  LOG(INFO) << mailbox_->GetQueueMapSize() << " threads are registered to node:" << node_.id;
  mailbox_->Start();
  VLOG(1) << "mailbox starts on node" << node_.id;
}


void Engine::StopWorkerHelperThreads() {
  worker_helper_thread_->Stop();
  VLOG(1) << "worker_helper_thread stops on node" << node_.id;
}

void Engine::StopServerThreads() {
  for (auto& server_thread : *server_thread_group_) {
    server_thread->Stop();
  }
  VLOG(1) << "server_threads stop on node" << node_.id;
}

void Engine::StopSender() {
  sender_->Stop();
  VLOG(1) << "sender stops on node" << node_.id;
}

void Engine::StopMailbox() {
  mailbox_->Stop();
  VLOG(1) << "mailbox stops on node" << node_.id;
}

void Engine::AllocateWorkers(WorkerSpec* const worker_spec) {
  CHECK(id_mapper_);
  for (auto& kv : worker_spec->GetNodeToWorkers()) {
    for (int i = 0; i < kv.second.size(); ++ i) {
      uint32_t tid = id_mapper_->AllocateWorkerThread(kv.first);
      worker_spec->InsertWorkerIdThreadId(kv.second[i], tid);
    }
  }
}


void Engine::CreateTable(uint32_t table_id,
    const std::vector<third_party::Range>& ranges,
    const std::vector<uint32_t>& worker_threads) {
  CHECK(id_mapper_);
  auto server_thread_ids = id_mapper_->GetAllServerThreads();
  CHECK_EQ(ranges.size(), server_thread_ids.size());
  SimpleRangeManager range_manager(ranges, server_thread_ids);
  CHECK(server_thread_group_);
  CHECK(range_manager_map_.find(table_id) == range_manager_map_.end());
  range_manager_map_.insert({table_id, range_manager});
  const int model_staleness = 1;  // TODO
  for (auto& server_thread : *server_thread_group_) {
    std::unique_ptr<AbstractStorage> storage(new Storage<int>());
    std::unique_ptr<AbstractModel> model(new SSPModel(table_id, std::move(storage), model_staleness,
                                                      server_thread_group_->GetReplyQueue()));
    model->ResetWorker(worker_threads);
    server_thread->RegisterModel(table_id, std::move(model));
  }
}

void Engine::Run(const MLTask& task) {
  const auto& worker_spec = task.GetWorkerSpec();
  if (worker_spec.HasLocalWorkers(node_.id)) {
    const auto& local_workers = worker_spec.GetLocalWorkers(node_.id);
    std::vector<std::thread> thread_group(local_workers.size());
    for (int i = 0; i < thread_group.size(); ++ i) {
      LOG(INFO) << thread_group.size() << " workers run on proc: " << node_.id;
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

void Engine::Barrier() {
  CHECK(mailbox_);
  mailbox_->Barrier();
}

}  // namespace flexps
