#include "driver/engine.hpp"

#include "server/storage.hpp"
#include "server/ssp_model.hpp"

#include <thread>
#include <vector>

#include "glog/logging.h"

namespace flexps {

void Engine::StartEverything() {
  CreateMailbox();
  StartSender();
  StartServerThreads();
  StartWorkerHelperThreads();
  StartModelInitThread();
  StartMailbox();
}

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

void Engine::StartModelInitThread() {
  CHECK(id_mapper_);
  CHECK(mailbox_);
  uint32_t model_init_thread_id = id_mapper_->GetModelInitThreadForId(node_.id);
  model_init_thread_.reset(new ModelInitThread(model_init_thread_id, sender_->GetMessageQueue()));
  mailbox_->RegisterQueue(model_init_thread_->GetThreadId(), model_init_thread_->GetWorkQueue());
  model_init_thread_->Start();
  VLOG(1) << "model_init_thread:" << model_init_thread_->GetThreadId() << " starts on node:" << node_.id;
}

void Engine::StartMailbox() {
  CHECK(mailbox_);
  LOG(INFO) << mailbox_->GetQueueMapSize() << " threads are registered to node:" << node_.id;
  mailbox_->Start();
  VLOG(1) << "mailbox starts on node" << node_.id;
}

void Engine::StopEverything() {
  StopSender();
  StopMailbox();
  StopServerThreads();
  StopWorkerHelperThreads();
  StopModelInitThread();
}

void Engine::StopWorkerHelperThreads() {
  CHECK(worker_helper_thread_);
  worker_helper_thread_->Stop();
  VLOG(1) << "worker_helper_thread stops on node" << node_.id;
}

void Engine::StopServerThreads() {
  CHECK(server_thread_group_);
  for (auto& server_thread : *server_thread_group_) {
    server_thread->Stop();
  }
  VLOG(1) << "server_threads stop on node" << node_.id;
}

void Engine::StopModelInitThread() {
  CHECK(model_init_thread_);
  model_init_thread_->Stop();
  VLOG(1) << "model_init_thread stops on node" << node_.id;
}

void Engine::StopSender() {
  CHECK(sender_);
  sender_->Stop();
  VLOG(1) << "sender stops on node" << node_.id;
}

void Engine::StopMailbox() {
  CHECK(mailbox_);
  mailbox_->Stop();
  VLOG(1) << "mailbox stops on node" << node_.id;
}

WorkerSpec Engine::AllocateWorkers(const std::vector<WorkerAlloc>& worker_alloc) {
  CHECK(id_mapper_);
  WorkerSpec worker_spec(worker_alloc);
  for (auto& kv : worker_spec.GetNodeToWorkers()) {
    for (int i = 0; i < kv.second.size(); ++ i) {
      uint32_t tid = id_mapper_->AllocateWorkerThread(kv.first);
      worker_spec.InsertWorkerIdThreadId(kv.second[i], tid);
    }
  }
  return worker_spec;
}


void Engine::CreateTable(uint32_t table_id,
    const std::vector<third_party::Range>& ranges) {
  CHECK(id_mapper_);
  auto server_thread_ids = id_mapper_->GetAllServerThreads();
  CHECK_EQ(ranges.size(), server_thread_ids.size());
  SimpleRangeManager range_manager(ranges, server_thread_ids);
  CHECK(server_thread_group_);
  CHECK(range_manager_map_.find(table_id) == range_manager_map_.end());
  range_manager_map_.insert({table_id, range_manager});
  const int model_staleness = 1;  // TODO
  for (auto& server_thread : *server_thread_group_) {
    std::unique_ptr<AbstractStorage> storage(new Storage<float>());
    std::unique_ptr<AbstractModel> model(new SSPModel(table_id, std::move(storage), model_staleness,
                                                      server_thread_group_->GetReplyQueue()));
    server_thread->RegisterModel(table_id, std::move(model));
  }
}

void Engine::InitTable(uint32_t table_id, const std::vector<uint32_t>& worker_ids) {
  CHECK(model_init_thread_);
  std::vector<uint32_t> local_servers = id_mapper_->GetServerThreadsForId(node_.id);
  model_init_thread_->ResetWorkerInModel(table_id, local_servers, worker_ids);
  Barrier();  // TODO: Now for each InitTable we will invoke a barrier.
}

void Engine::Run(const MLTask& task) {
  CHECK(task.IsSetup());
  const std::vector<uint32_t>& tables = task.GetTables();
  WorkerSpec worker_spec = AllocateWorkers(task.GetWorkerAlloc());
  // Init tables
  for (auto table : tables) {
    InitTable(table, worker_spec.GetThreadIds());
  }
  if (worker_spec.HasLocalWorkers(node_.id)) {
    const auto& local_threads = worker_spec.GetLocalThreads(node_.id);
    const auto& local_workers = worker_spec.GetLocalWorkers(node_.id);
    CHECK_EQ(local_threads.size(), local_workers.size());
    std::vector<std::thread> thread_group(local_threads.size());
    for (int i = 0; i < thread_group.size(); ++ i) {
      // TODO:
      mailbox_->RegisterQueue(local_threads[i], worker_helper_thread_->GetWorkQueue());
      LOG(INFO) << thread_group.size() << " workers run on proc: " << node_.id;
      Info info;
      info.thread_id = local_threads[i];
      info.worker_id = local_workers[i];
      info.send_queue = sender_->GetMessageQueue();
      info.range_manager_map = range_manager_map_;  // Now I just copy it
      info.callback_runner = app_blocker_.get();
      thread_group[i] = std::thread([&task, info](){
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
