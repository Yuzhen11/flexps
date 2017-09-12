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
      std::unique_ptr<AbstractModel> model(
          new SSPModel(i, tids, std::move(storage), model_staleness, server_thread->GetWorkQueue()));
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

  // ADD msg
  {
    Message m1;
    m1.meta.flag = Flag::kAdd;
    m1.meta.model_id = 0;
    m1.meta.sender = 2;
    m1.meta.recver = 0;
    third_party::SArray<int> m1_keys({0});
    third_party::SArray<int> m1_vals({1});
    m1.AddData(m1_keys);
    m1.AddData(m1_vals);

    Message m2;
    m2.meta.flag = Flag::kAdd;
    m2.meta.model_id = 0;
    m2.meta.sender = 3;
    m2.meta.recver = 0;
    third_party::SArray<int> m2_keys({1});
    third_party::SArray<int> m2_vals({2});
    m2.AddData(m2_keys);
    m2.AddData(m2_vals);

    messages.push_back(m1);
    messages.push_back(m2);
  }

  // GET msg
  {
    // Message1
    Message m1;
    m1.meta.flag = Flag::kGet;
    m1.meta.model_id = 0;
    m1.meta.sender = 2;
    m1.meta.recver = 0;
    third_party::SArray<int> m1_keys({0});
    m1.AddData(m1_keys);

    // Message2
    Message m2;
    m2.meta.flag = Flag::kGet;
    m2.meta.model_id = 0;
    m2.meta.sender = 3;
    m2.meta.recver = 0;
    third_party::SArray<int> m2_keys({1});
    m2.AddData(m2_keys);
  }

  // EXIT msg
  {
    Message exit_msg;
    exit_msg.meta.flag = Flag::kExit;
    server_queues[0]->Push(exit_msg);
    server_queues[1]->Push(exit_msg);
  }

  // Stop
  for (auto& server_thread : server_thread_group) {
    server_thread->Stop();
  }
}
}  // namespace flexps

int main() { flexps::TestServer(); }
