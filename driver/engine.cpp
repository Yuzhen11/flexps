#include "driver/engine.hpp"

#include <thread>
#include <vector>

#include "glog/logging.h"

namespace flexps {

void Engine::StartEverything(int num_server_thread_per_node) {
  CreateIdMapper(num_server_thread_per_node);
  CreateMailbox();
  StartSender();
  StartServerThreads();
  StartWorkerHelperThreads();
  StartMailbox();
  LOG(INFO) << "StartEverything in Node: " << node_.id;
}

void Engine::CreateIdMapper(int num_server_thread_per_node) {
  id_mapper_.reset(new SimpleIdMapper(node_, nodes_));
  id_mapper_->Init(num_server_thread_per_node);
}
void Engine::CreateMailbox() { mailbox_.reset(new Mailbox(node_, nodes_, id_mapper_.get())); }

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
  CHECK(mailbox_);
  VLOG(1) << mailbox_->GetQueueMapSize() << " threads are registered to node:" << node_.id;
  mailbox_->Start();
  VLOG(1) << "mailbox starts on node" << node_.id;
}

void Engine::StopEverything() {
  StopMailbox();
  StopSender();
  StopServerThreads();
  StopWorkerHelperThreads();
  LOG(INFO) << "StopEverything in Node: " << node_.id;
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
  // Need to make sure that all the engines allocate the same set of workers
  for (auto& kv : worker_spec.GetNodeToWorkers()) {
    for (int i = 0; i < kv.second.size(); ++i) {
      uint32_t tid = id_mapper_->AllocateWorkerThread(kv.first);
      worker_spec.InsertWorkerIdThreadId(kv.second[i], tid);
    }
  }
  return worker_spec;
}

void Engine::RegisterRangePartitionManager(uint32_t table_id, const std::vector<third_party::Range>& ranges) {
  CHECK(id_mapper_);
  auto server_thread_ids = id_mapper_->GetAllServerThreads();
  CHECK_EQ(ranges.size(), server_thread_ids.size());
  std::unique_ptr<SimpleRangePartitionManager> range_manager(
      new SimpleRangePartitionManager(ranges, server_thread_ids));
  CHECK(partition_manager_map_.find(table_id) == partition_manager_map_.end());
  partition_manager_map_[table_id] = std::move(range_manager);
}

void Engine::InitTable(uint32_t table_id, const std::vector<uint32_t>& worker_ids) {
  CHECK(id_mapper_);
  CHECK(mailbox_);
  std::vector<uint32_t> local_servers = id_mapper_->GetServerThreadsForId(node_.id);
  int count = local_servers.size();
  if (count == 0)
    return;
  // Register receiving queue
  auto id = id_mapper_->AllocateWorkerThread(node_.id);  // TODO allocate background thread?
  ThreadsafeQueue<Message> queue;
  mailbox_->RegisterQueue(id, &queue);
  // Create and send reset worker message
  Message reset_msg;
  reset_msg.meta.flag = Flag::kResetWorkerInModel;
  reset_msg.meta.model_id = table_id;
  reset_msg.meta.sender = id;
  reset_msg.AddData(third_party::SArray<uint32_t>(worker_ids));
  for (auto local_server : local_servers) {
    reset_msg.meta.recver = local_server;
    sender_->GetMessageQueue()->Push(reset_msg);
  }
  // Wait for reply
  Message reply;
  while (count > 0) {
    queue.WaitAndPop(&reply);
    CHECK(reply.meta.flag == Flag::kResetWorkerInModel);
    CHECK(reply.meta.model_id == table_id);
    --count;
  }
  // Free receiving queue
  mailbox_->DeregisterQueue(id);
  id_mapper_->DeallocateWorkerThread(node_.id, id);
}

void Engine::Run(const MLTask& task) {
  CHECK(task.IsSetup());
  WorkerSpec worker_spec = AllocateWorkers(task.GetWorkerAlloc());

  // Init tables
  const std::vector<uint32_t>& tables = task.GetTables();
  for (auto table : tables) {
    InitTable(table, worker_spec.GetAllThreadIds());
  }
  Barrier();

  // Spawn user threads
  if (worker_spec.HasLocalWorkers(node_.id)) {
    const auto& local_threads = worker_spec.GetLocalThreads(node_.id);
    const auto& local_workers = worker_spec.GetLocalWorkers(node_.id);
    CHECK_EQ(local_threads.size(), local_workers.size());
    std::vector<std::thread> thread_group(local_threads.size());
    LOG(INFO) << thread_group.size() << " workers run on proc: " << node_.id;
    std::map<uint32_t, AbstractPartitionManager*> partition_manager_map;
    for (auto& table : tables) {
      auto it = partition_manager_map_.find(table);
      CHECK(it != partition_manager_map_.end());
      partition_manager_map[table] = it->second.get();
    }
    for (int i = 0; i < thread_group.size(); ++i) {
      // TODO: Now I register the thread_id with the queue in worker_helper_thread to the mailbox.
      // So that the message sent to thread_id will be pushed into worker_helper_thread_'s queue
      // and worker_helper_thread_ is in charge of handling the message.
      mailbox_->RegisterQueue(local_threads[i], worker_helper_thread_->GetWorkQueue());
      Info info;
      info.local_id = i;
      info.thread_id = local_threads[i];
      info.worker_id = local_workers[i];
      info.send_queue = sender_->GetMessageQueue();
      info.partition_manager_map = partition_manager_map;
      info.callback_runner = app_blocker_.get();
      info.mailbox = mailbox_.get();
      thread_group[i] = std::thread([&task, info]() { task.RunLambda(info); });
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
