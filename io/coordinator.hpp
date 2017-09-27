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
#include "zmq_helper.hpp"

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

};

}  
