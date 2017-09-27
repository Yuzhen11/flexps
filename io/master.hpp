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

#pragma once

#include <string>
#include <memory>
#include <unordered_map>
#include <unordered_set>
#include <utility>
#include <vector>
#include <glog/logging.h>
#include "zmq.hpp"
#include "zmq_helper.hpp"
namespace flexps {

class Master {
   public:
    static Master& get_instance() {
        static Master master;
        return master;
    }

    virtual ~Master() = default;

    void setup(int master_port, zmq::context_t* context);
    void init_socket(int master_port, zmq::context_t* context);
    void serve();
    void handle_message(uint32_t message, const std::string& id);

    inline void halt() { running = false; }
    inline std::shared_ptr<zmq::socket_t> get_socket() const { return master_socket; }
    inline const std::string& get_cur_client() const { return cur_client; }
    inline void register_main_handler(uint32_t msg_type, std::function<void()> handler) {
        external_main_handlers[msg_type] = handler;
    }

    inline void register_setup_handler(std::function<void()> handler) {
        external_setup_handlers.push_back(handler); 
    }

   protected:
    Master();
    bool running;
    std::string cur_client;
    // Networking
    std::shared_ptr<zmq::socket_t> master_socket;
    // External handlers
    std::unordered_map<uint32_t, std::function<void()>> external_main_handlers;
    std::vector<std::function<void()>> external_setup_handlers;

};

}  // namespace flexps
