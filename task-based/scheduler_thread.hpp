#pragma once

#include "base/message.hpp"
#include "base/threadsafe_queue.hpp"
#include "comm/abstract_mailbox.hpp"
#include "glog/logging.h"

#include <thread>
#include <set>

namespace flexps {

class SchedulerThread {

public:
    SchedulerThread(AbstractMailbox* const mailbox);
    ~SchedulerThread();
    void RegisterWorker(uint32_t worker_id);

    void Start();
    void Stop();
    void Send(const Message& msg);
    ThreadsafeQueue<Message>* GetWorkQueue();
    std::set<uint32_t> GetWorkerIDs();
    void Main();

private:
    // owned
    uint32_t scheduler_id_ = 0; // for now, the schduler's id is always 0
    std::thread work_thread_;
    ThreadsafeQueue<Message> work_queue_;
    std::set<uint32_t> worker_ids_;

    // Not owned.
    AbstractMailbox* const mailbox_;
};

}  // namespace flexps
