#pragma once

#include <sstream>

#include "base/threadsafe_queue.hpp"
#include "worker/simple_range_manager.hpp"
#include "worker/abstract_callback_runner.hpp"

namespace flexps {

struct Info {
  uint32_t thread_id;
  uint32_t worker_id;
  ThreadsafeQueue<Message>* send_queue;
  std::map<uint32_t, SimpleRangeManager> range_manager_map;
  AbstractCallbackRunner* callback_runner;
  std::string DebugString() const {
    std::stringstream ss;
    ss << "thread_id: " << thread_id << " worker_id: " << worker_id;
    return ss.str();
  }
};

}  // namespace flexps
