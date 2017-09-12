#include "glog/logging.h"
#include "gtest/gtest.h"

#include "server/server_thread.hpp"
#include "server/server_thread_group.hpp"
#include "server/ssp_model.hpp"
#include "server/storage.hpp"

#include <iostream>

namespace flexps {


void TestServer() {
  // Create a group of ServerThread
  std::vector<int> server_id_vec{0, 1};
  ServerThreadGroup server_thread_group(server_id_vec);

  // std::thread's copy constructor is deleted
  // std::vector<ServerThread> server_thread_group{ServerThread(0), ServerThread(1)};

  // Create models and register models into ServerThread
  const int num_models = 3;
  int model_staleness = 1;
  std::vector<int> tids{2, 3};
  for (int i = 0; i < num_models; ++i) {
    for (auto& server_thread : server_thread_group) {
      // TODO(Ruoyu Wu): Each server thread should have its own model?
      std::unique_ptr<AbstractStorage> storage(new Storage<int>());
      std::unique_ptr<AbstractModel> model(new SSPModel(i, tids, std::move(storage), model_staleness, server_thread->GetWorkQueue()));
      server_thread->RegisterModel(i, std::move(model));
    }
  }

  // Collect server queues
  std::map<uint32_t, ThreadsafeQueue<Message>*> server_queues;
  for (auto& server_thread : server_thread_group) {
    server_queues.insert({server_thread->GetServerId(), server_thread->GetWorkQueue()});
  }

  // Dispatch messages to queues
  std::vector<Message> messages;
  
  for (auto& msg : messages) {
    CHECK(server_queues.find(msg.meta.recver) != server_queues.end());
    // TODO(Ruoyu Wu): server_queues should check the msg sender?
    server_queues[msg.meta.recver]->Push(msg);
  }

  // Start
  for (auto& server_thread : server_thread_group) {
    server_thread->Start();
  }

  // Stop
  for (auto& server_thread : server_thread_group) {
    server_thread->Stop();
  }

}
}  // namespace flexps

// int main() { flexps::TestServer(); }
