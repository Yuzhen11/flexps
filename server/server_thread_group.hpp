#pragma once

#include <vector>

#include "server/server_thread.hpp"

namespace flexps {

class ServerThreadGroup {
 public:
  ServerThreadGroup(std::vector<int>& server_id_vec, ThreadsafeQueue<Message>* reply_queue)
      : reply_queue_(reply_queue) {
    for (auto& server_id : server_id_vec)
      server_threads.push_back(new ServerThread(server_id));
  }

  ThreadsafeQueue<Message>* GetReplyQueue() { return &reply_queue_; }

  std::vector<ServerThread*>::iterator begin() { return server_threads.begin(); }

  std::vector<ServerThread*>::iterator end() { return server_threads.end(); }

 private:
  std::vector<ServerThread*> server_threads;
  ThreadsafeQueue<Message>* reply_queue_;
};

}  // namespace flexps