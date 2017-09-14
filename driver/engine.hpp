#pragma once

#include <memory>
#include <vector>

#include "base/node.hpp"
#include "worker/worker_helper_thread.hpp"
#include "server/server_thread.hpp"
#include "comm/mailbox.hpp"
#include "driver/ml_task.hpp"

namespace flexps {

class Engine {
 public:
  Engine(const Node& node, const std::vector<Node>& nodes) : node_(node), nodes_(nodes) {}
  /*
   * The flow of starting the engine:
   * 1. Create a mailbox
   * 2. Create Sender
   * 3. Create ServerThreads and WorkerHelperThreads
   * 4. Register the threads to mailbox through ThreadsafeQueue
   * 5. Start the mailbox: bind and connect to all other nodes
   */
  void CreateMailbox();
  void CreateSender();
  void StartServerThreads();
  void StartWorkerHelperThreads();
  void StartMailbox();
  void StartSender();

  /*
   * The flow of stopping the engine:
   * 1. Stop the mailbox: by sending each node an exit message
   * 2. The mailbox will stop the corresponding registered threads
   * 3. Join the ServerThreads and WorkerHelperThreads
   * 4. Join the Sender
   * 5. Join the mailbox
   */
  void StopServerThreads();
  void StopWorkerHelperThreads();
  void StopSender();
  void StopMailbox();
 
  void CreateTable(uint32_t table_id, const SimpleRangeManager& range_manager);
  void Run(const MLTask& task);
 private:
  std::map<uint32_t, SimpleRangeManager> range_manager_map_;
  Node node_;
  std::vector<Node> nodes_;
  // mailbox
  std::unique_ptr<Mailbox> mailbox_;
  // worker elements
  std::unique_ptr<AppBlocker> app_blocker_;
  std::unique_ptr<WorkerHelperThread> worker_helper_thread_;
  std::unique_ptr<ThreadsafeQueue<Message>> worker_send_queue_;
  // server elements
  std::unique_ptr<ServerThreadGroup> server_thread_group_;
};

}  // namespace flexps
