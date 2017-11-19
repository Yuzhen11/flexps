#include "task/thread_template.hpp"

#include <set>

namespace flexps {

class SchedulerThread public ThreadTemplate {

public:
    SchedulerThread(AbstractMailbox* const mailbox) : id_(0), mailbox_(mailbox) {
        mailbox_->RegisterQueue(id_, &work_queue_);
    }

    void SchedulerThread::RegisterWorker(uint32_t worker_id) {
        CHECK(worker_ids_.find(worker_id) == worker_ids_.end()) << "Worker " << worker_id << "already registered to scheduler";
        worker_ids_.insert(worker_id);
    }

    std::set<uint32_t> GetWorkerIds() { return worker_ids_; }

    void Main() override {
        while (true) {
            Message msg;
            work_queue_.WaitAndPop(&msg);
            CHECK(msg.meta.recver == id_);
            CHECK(worker_ids_.find(msg.meta.send) != worker_ids_.end()) << "Received message from unknown thread";

            LOG(INFO) << msg.DebugString();
            if (msg.meta.flag == Flag::kExit)
                break;
        }
    }

private:
    // owned
    std::set<uint32_t> worker_ids_;
};

}  // namespace flexps
