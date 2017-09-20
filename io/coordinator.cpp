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

#include "coordinator.hpp"
#include <string>
#include "glog/glog.h"
#include "base/serialization.hpp"
namespace flexps {

Coordinator::Coordinator(int proc_id, std::string hostname, zmq::context_t* context, std::string master_host, int master_port): 
proc_id_(proc_id), hostname_(hostname), context_(context), master_host_(master_host), master_port_(master_port)
{}

Coordinator::~Coordinator() {
    delete zmq_coord_;
    zmq_coord_ = nullptr;
}

void Coordinator::serve() {
    if (zmq_coord_)
        return;

    std::string hostname = hostname_ + "-" + std::to_string(proc_id_);

    zmq_coord_ = new zmq::socket_t(context_, ZMQ_DEALER);
    zmq_coord_->setsockopt(ZMQ_IDENTITY, hostname.c_str(), hostname.size());
    int linger = 2000;
    zmq_coord_->setsockopt(ZMQ_LINGER, &linger, sizeof(linger));
    zmq_coord_->connect("tcp://" + master_host_ + ":" +
                        std::to_string(master_port_));
}

base::BinStream Coordinator::ask_master(base::BinStream& question, size_t type) {
    coord_lock_.lock();

    // Question type
    zmq_send_common(zmq_coord_, nullptr, 0, ZMQ_SNDMORE); //
    zmq_send_common(zmq_coord_, &type, sizeof(int32_t), ZMQ_SNDMORE);
    // Question body
    zmq_send_common(zmq_coord_, question.get_remained_buffer(), question.size());
    zmq::message_t msg1,msg2;
    base::BinStream answer;
    //receive notification
    zmq_recv_common(zmq_coord_, &msg1);
    //receive answer to question
    zmq_recv_common(zmq_coord_, &msg2);
    answer.push_back_bytes(reinterpret_cast<char*>(msg.data()), msg.size());
    coord_lock_.unlock();

    return answer;
}

void Coordinator::notify_master(base::BinStream& message, size_t type) {
    coord_lock_.lock();

    // type
    zmq_send_common(zmq_coord_, nullptr, 0, ZMQ_SNDMORE);
    //zmq_sendmore_int32(zmq_coord_, type);
    // Message body
    zmq_send_common(zmq_coord_, message.get_remained_buffer(), message.size());
    coord_lock_.unlock();
}




inline void zmq_send_common(zmq::socket_t* socket, const void* data, const size_t& len, int flag = ZMQ_BLOCKING) {
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

inline void zmq_recv_common(zmq::socket_t* socket, zmq::message_t* msg, int flag = ZMQ_BLOCKING) {
    CHECK(socket != nullptr) << "zmq::socket_t cannot be nullptr!";
    CHECK(msg != nullptr || len == 0) << "data and len are not matched!";
    while (true)
        try {
            bool successful = socket->recv(msg, flag);
            CHECK(buccessful) << "zmq::receive error!";
            break;
        } catch (zmq::error_t e) {
            if (e.num() == EINTR)
                continue;
            LOG(ERROR) << "Invalid type of zmq::error!";
        }
}





}  // namespace husky