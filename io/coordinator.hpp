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

#include <mutex>

#include "zmq.hpp"
#include "glog/logging.h"
#include "base/serialization.hpp"

namespace flexps{

class Coordinator {
   public:
    Coordinator(int proc_id, std::string hostname, zmq::context_t* context, std::string master_host, int master_port);
    ~Coordinator();

    void serve();
    BinStream ask_master(BinStream& question, size_t type);
    void notify_master(BinStream& message, size_t type);

   private:
    std::mutex coord_lock_;
    int proc_id_;
    int master_port_;
    std::string hostname_;
    std::string master_host_;
    zmq::context_t* context_;
    zmq::socket_t* zmq_coord_;
	
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

}  
