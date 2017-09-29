// Copyright 2016 Husky Team
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#include "master.hpp"

#include <set>
#include <string>
#include <unordered_map>
#include <unordered_set>
#include <utility>
#include <vector>

#include "zmq.hpp"

#include "base/serialization.hpp"
#include "glog/logging.h"

namespace flexps {

Master::Master() {}

void Master::setup(int master_port, zmq::context_t* zmq_context) {
  running = true;
  init_socket(master_port, zmq_context);

  for (auto setup_handler : external_setup_handlers) {
    setup_handler();
  }
}

void Master::init_socket(int master_port, zmq::context_t* zmq_context) {
  master_socket.reset(new zmq::socket_t(*zmq_context, ZMQ_ROUTER));
  master_socket->bind("tcp://*:" + std::to_string(master_port));
}

void Master::serve() {
  LOG(INFO) << "MASTER READY";
  while (running) {
    zmq::message_t msg1, msg2, msg3;
    zmq_recv_common(master_socket.get(), &msg1);
    cur_client = std::string(reinterpret_cast<char*>(msg1.data()), msg1.size());
    zmq_recv_common(master_socket.get(), &msg2);
    zmq_recv_common(master_socket.get(), &msg3);
    int msg_int = *reinterpret_cast<int32_t*>(msg3.data());
    handle_message(msg_int, cur_client);
  }
  LOG(INFO) << "MASTER FINISHED";
}

void Master::handle_message(uint32_t message, const std::string& id) {
  if (external_main_handlers.find(message) != external_main_handlers.end()) {
    external_main_handlers[message]();
  } else {
    LOG(ERROR) << "Unknown message type!";
  }
}
}  // namespace flexps
