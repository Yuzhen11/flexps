#pragma once

#include "base/message.hpp"
#include "base/threadsafe_queue.hpp"
#include "comm/abstract_mailbox.hpp"
#include "glog/logging.h"

#include <thread>

namespace flexps {

class WorkerThread {

public:
    WorkerThread(uint32_t worker_id, AbstractMailbox* const mailbox);
    ~WorkerThread();

    void Start();
    void Stop();
    void Send(const Message& msg);
    ThreadsafeQueue<Message>* GetWorkQueue();
    uint32_t GetWorkerId() const;
    void Main();

private:
    uint32_t worker_id_;
    uint32_t scheduler_id_ = 0; // schduler's id is always 0 for now
    std::thread work_thread_;
    ThreadsafeQueue<Message> work_queue_;

    // not owned
    AbstractMailbox* const mailbox_;
};

}  // namespace flexps
