#include "app_blocker.hpp"

#include "glog/logging.h"

namespace flexps {

void AppBlocker::SanityCheck(uint32_t app_thread_id, uint32_t model_id) {
  CHECK(recv_handle_.find(app_thread_id) != recv_handle_.end()) 
    << "recv_handle_ for app_thread_id:" << app_thread_id << " is not registered";
  CHECK(recv_handle_[app_thread_id].find(model_id) != recv_handle_[app_thread_id].end()) 
    << "recv_handle_ for model:" << model_id << " is not registered";

  CHECK(recv_finish_handle_.find(app_thread_id) != recv_finish_handle_.end()) 
    << "recv_finish_handle_ for model:" << app_thread_id << " is not registered";
  CHECK(recv_finish_handle_[app_thread_id].find(model_id) != recv_finish_handle_[app_thread_id].end()) 
    << "recv_finish_handle_ for model:" << model_id << " is not registered";
}

void AppBlocker::RegisterRecvHandle(uint32_t app_thread_id, uint32_t model_id, const std::function<void(Message&)>& recv_handle) {
  std::lock_guard<std::mutex> lk(mu_);
  recv_handle_[app_thread_id][model_id] = recv_handle;
}

void AppBlocker::RegisterRecvFinishHandle(uint32_t app_thread_id, uint32_t model_id, const std::function<void(Message&)>& recv_finish_handle) {
  std::lock_guard<std::mutex> lk(mu_);
  recv_finish_handle_[app_thread_id][model_id] = recv_finish_handle;
}

void AppBlocker::NewRequest(uint32_t app_thread_id, uint32_t model_id, uint32_t expected_responses) {
  std::lock_guard<std::mutex> lk(mu_);
  SanityCheck(app_thread_id, model_id);
  tracker_[app_thread_id][model_id] = {expected_responses, 0};
}
void AppBlocker::WaitRequest(uint32_t app_thread_id, uint32_t model_id) {
  std::unique_lock<std::mutex> lk(mu_);
  SanityCheck(app_thread_id, model_id);
  cond_.wait(lk, [this, app_thread_id, model_id] {
    return tracker_[app_thread_id][model_id].first == tracker_[app_thread_id][model_id].second;
  });
}
void AppBlocker::AddResponse(uint32_t app_thread_id, uint32_t model_id, Message& msg) {
  bool recv_finish = false;
  {
    std::lock_guard<std::mutex> lk(mu_);
    SanityCheck(app_thread_id, model_id);
    tracker_[app_thread_id][model_id].second += 1;
    if (tracker_[app_thread_id][model_id].first == tracker_[app_thread_id][model_id].second) {
      recv_finish = true;
    }
  }
  recv_handle_[app_thread_id][model_id](msg);
  if (recv_finish) {
    recv_finish_handle_[app_thread_id][model_id](msg);
    cond_.notify_all();
  }
}

}  // namespace flexps
