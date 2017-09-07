#include "server/server_thread.hpp"
#include "server/ssp_model.hpp"
#include "server/storage.hpp"

namespace flexps {

/*
 * An example about how to use the stuff on the server side.
 */
void TestServer() {
  // Create a group of ServerThread
  std::vector<ServerThread> server_thread_group{ServerThread(0), ServerThread(1)};

  // Create models and register models into ServerThread
  const int num_models = 3;
  for (int i = 0; i < num_models; ++i) {
    auto* p_model = new SSPModel(i);
    // Setup
    p_model->Init(...);
    std::unique_ptr<AbstractModel> model(p_model);
    for (auto& server_thread : server_thread_group) {
      server_thread.RegisterModel(i, std::move(model));
    }
  }

  // Collect server queues
  std::map<uint32_t, ThreadsafeQueue<Message>*> server_queues;
  for (auto& server_thread : server_thread_group) {
    server_queues.insert({server_thread.GetServerId(), server_thread.GetWorkQueue()});
  }

  // Dispatch messages to queues
  std::vector<Message> messages;
  {
    Message m;
    m.meta.flag = Flag::kClock;
    m.meta.model_id = model_id;
    m.meta.sender = 0;
    m.meta.recver = 0;
    messages.push_back(m);
  }
  for (auto& msg : messages) {
    CHECK(server_queues.find(msg.meta.recver) != server_queues.end());
    server_queues[msg.meta.recver]->Push(msg);
  }
}

}  // namespace flexps

int main() { flexps::TestServer(); }
