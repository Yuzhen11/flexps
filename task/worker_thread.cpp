#include "task/thread_template.hpp"

namespace flexps {

class WorkerThread public ThreadTemplate {

public:
    WorkerThread(uint32_t id, AbstractMailbox* const mailbox) : id_(id), mailbox_(mailbox) {
        CHECK(id != 0) << "id 0 is reserved for scheduler_thread";
        mailbox_->RegisterQueue(id, &work_queue_);
    }

    void Send(const Message& msg) override {
		CHECK(msg.meta.recver == 0) << "worker can only send message to scheduler for now";
		mailbox_->Send(msg);
	}

	void Main() override {
        while (true) {
            Message msg;
            work_queue_.WaitAndPop(&msg);
            CHECK(msg.meta.recver == id_);
            CHECK(msg.meta.sender != 0) << "Received message not from scheduler";

            LOG(INFO) << msg.DebugString();
            if (msg.meta.flag == Flag::kExit)
                break;
        }
    }
};

}  // namespace flexps
