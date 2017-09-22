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
#include <unordered_map>
#include <unordered_set>
#include <utility>
#include <vector>

#include "zmq.hpp"

namespace flexps {

class Master {
   public:
    static Master& get_instance() {
        static Master master;
        return master;
    }

    virtual ~Master() = default;

    void setup();
    void init_socket();
    void serve();
    void handle_message(uint32_t message, const std::string& id);

    inline void halt() { running = false; }
    inline std::shared_ptr<zmq::socket_t> get_socket() const { return master_socket; }
    inline const std::string& get_cur_client() const { return cur_client; }
    inline void register_main_handler(uint32_t msg_type, std::function<void()> handler) {
        external_main_handlers[msg_type] = handler;
    }

    inline void register_setup_handler(std::function<void()> handler) { external_setup_handlers.push_back(handler); }

   protected:
    Master();
    bool running;
    std::string cur_client;
    // Networking
    zmq::context_t zmq_context;
    std::shared_ptr<zmq::socket_t> master_socket;

    // External handlers
    std::unordered_map<uint32_t, std::function<void()>> external_main_handlers;
    std::vector<std::function<void()>> external_setup_handlers;



    inline void zmq_send_common(zmq::socket_t* socket, const void* data, const size_t& len, int flag = !ZMQ_NOBLOCK) {
        CHECK(socket != nullptr) << "zmq::socket_t cannot be nullptr!";
        CHECK(data != nullptr || len == 0) << "data and len are not matched!";
        while (true)
            try {
                size_t bytes = socket->send(data, len, flag);
                CHECK(bytes == len) << "zmq::send error!";
                break;
            } catch (zmq::error_t e) {
            switch (e.num()) {
                case EHOSTUNREACH:
                case EINTR:
                continue;
            default:
                LOG(ERROR) << "Invalid type of zmq::error!";
            }
        }
    }

    inline void zmq_recv_common(zmq::socket_t* socket, zmq::message_t* msg, int flag = !ZMQ_NOBLOCK) {
        CHECK(socket != nullptr) << "zmq::socket_t cannot be nullptr!";
        CHECK(msg != nullptr) << "data and len are not matched!";
        while (true)
            try {
                bool successful = socket->recv(msg, flag);
                CHECK(successful) << "zmq::receive error!";
                break;
            } catch (zmq::error_t e) {
            if (e.num() == EINTR)
                continue;
            LOG(ERROR) << "Invalid type of zmq::error!";
        }
    }

};

}  // namespace flexps