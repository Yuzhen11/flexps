#include "worker/abstract_callback_runner.hpp"

#include "glog/logging.h"

namespace flexps {

class FakeCallbackRunner : public AbstractCallbackRunner {
 public:
  FakeCallbackRunner(uint32_t kTestAppThreadId, uint32_t kTestModelId)
    : kTestAppThreadId_(kTestAppThreadId), kTestModelId_(kTestModelId){}
  virtual void RegisterRecvHandle(uint32_t app_thread_id, uint32_t model_id,
                                  const std::function<void(Message&)>& recv_handle) override {
    CHECK_EQ(app_thread_id, kTestAppThreadId_);
    CHECK_EQ(model_id, kTestModelId_);
    recv_handle_ = recv_handle;
  }
  virtual void RegisterRecvFinishHandle(uint32_t app_thread_id, uint32_t model_id,
                                        const std::function<void()>& recv_finish_handle) override {
    CHECK_EQ(app_thread_id, kTestAppThreadId_);
    CHECK_EQ(model_id, kTestModelId_);
    recv_finish_handle_ = recv_finish_handle;
  }

  virtual void NewRequest(uint32_t app_thread_id, uint32_t model_id, uint32_t expected_responses) override {
    CHECK_EQ(app_thread_id, kTestAppThreadId_);
    CHECK_EQ(model_id, kTestModelId_);
    tracker_ = {expected_responses, 0};
  }
  virtual void WaitRequest(uint32_t app_thread_id, uint32_t model_id) override {
    CHECK_EQ(app_thread_id, kTestAppThreadId_);
    CHECK_EQ(model_id, kTestModelId_);
    std::unique_lock<std::mutex> lk(mu_);
    cond_.wait(lk, [this] { return tracker_.first == tracker_.second; });
  }
  void AddResponse(Message m) {
    EXPECT_NE(recv_handle_, nullptr);
    bool recv_finish = false;
    {
      std::lock_guard<std::mutex> lk(mu_);
      recv_finish = tracker_.first == tracker_.second + 1 ? true : false;
    }
    recv_handle_(m);
    if (recv_finish) {
      recv_finish_handle_();
    }
    {
      std::lock_guard<std::mutex> lk(mu_);
      tracker_.second += 1;
      if (recv_finish) {
        cond_.notify_all();
      }
    }
  }

 private:
  std::function<void(Message&)> recv_handle_;
  std::function<void()> recv_finish_handle_;

  std::mutex mu_;
  std::condition_variable cond_;
  std::pair<uint32_t, uint32_t> tracker_;

  const uint32_t kTestAppThreadId_;
  const uint32_t kTestModelId_;
};

}  // namespace flexps

