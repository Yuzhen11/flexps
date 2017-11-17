#include "worker/worker_helper_thread.hpp"

#include "worker/app_blocker.hpp"
#include "worker/kv_client_table.hpp"
#include "worker/simple_range_manager.hpp"

#include <atomic>
#include <thread>

namespace flexps {

void TestBgWorker() {
  // Create app_blocker and worker_helper_thread
  AppBlocker app_blocker;
  WorkerHelperThread worker_helper_thread(0, &app_blocker);

  std::atomic<bool> ready(false);

  // Create a worker thread which runs the KVClientTable
  SimpleRangePartitionManager range_manager({{2, 4}, {4, 7}}, {0, 1});
  ThreadsafeQueue<Message> downstream_queue;

  const uint32_t kTestAppThreadId1 = 1;
  const uint32_t kTestAppThreadId2 = 2;
  const uint32_t kTestModelId = 59;
  std::thread worker_thread1(
      [kTestAppThreadId1, kTestModelId, &app_blocker, &downstream_queue, &range_manager, &ready]() {
        while (!ready) {
          std::this_thread::yield();
        }
        KVClientTable<float> table(kTestAppThreadId1, kTestModelId, &downstream_queue, &range_manager, &app_blocker);
        // Add
        std::vector<Key> keys = {3, 4, 5, 6};
        std::vector<float> vals = {0.1, 0.1, 0.1, 0.1};
        table.Add(keys, vals);  // {3,4,5,6} -> {3}, {4,5,6}

        // Get
        std::vector<float> rets;
        table.Get(keys, &rets);
        CHECK_EQ(rets.size(), 4);
        LOG(INFO) << rets[0] << " " << rets[1] << " " << rets[2] << " " << rets[3];
      });
  std::thread worker_thread2(
      [kTestAppThreadId2, kTestModelId, &app_blocker, &downstream_queue, &range_manager, &ready]() {
        while (!ready) {
          std::this_thread::yield();
        }
        KVClientTable<float> table(kTestAppThreadId2, kTestModelId, &downstream_queue, &range_manager, &app_blocker);
        // Add
        std::vector<Key> keys = {3, 4, 5, 6};
        std::vector<float> vals = {0.1, 0.1, 0.1, 0.1};
        table.Add(keys, vals);  // {3,4,5,6} -> {3}, {4,5,6}

        // Get
        std::vector<float> rets;
        table.Get(keys, &rets);
        CHECK_EQ(rets.size(), 4);
        LOG(INFO) << rets[0] << " " << rets[1] << " " << rets[2] << " " << rets[3];
      });

  auto* work_queue = worker_helper_thread.GetWorkQueue();

  worker_helper_thread.Start();
  // resume the worker thread when worker_helper_thread starts
  ready = true;

  // Consume messages in downstream_queue
  const int kNumMessages = 8;
  for (int i = 0; i < kNumMessages; ++i) {
    Message msg;
    downstream_queue.WaitAndPop(&msg);
    LOG(INFO) << msg.DebugString();
  }
  // response
  Message msg1, msg2;
  msg1.meta.recver = kTestAppThreadId1;
  msg1.meta.model_id = kTestModelId;
  msg1.meta.flag = Flag::kGet;
  msg2.meta.recver = kTestAppThreadId1;
  msg2.meta.model_id = kTestModelId;
  msg2.meta.flag = Flag::kGet;
  third_party::SArray<Key> r1_keys{3};
  third_party::SArray<float> r1_vals{0.1};
  msg1.AddData(r1_keys);
  msg1.AddData(r1_vals);
  third_party::SArray<Key> r2_keys{4, 5, 6};
  third_party::SArray<float> r2_vals{0.4, 0.2, 0.3};
  msg2.AddData(r2_keys);
  msg2.AddData(r2_vals);
  work_queue->Push(msg1);
  work_queue->Push(msg2);

  // To worker_thread2
  msg1.meta.recver = kTestAppThreadId2;
  msg1.meta.model_id = kTestModelId;
  msg2.meta.recver = kTestAppThreadId2;
  msg2.meta.model_id = kTestModelId;
  work_queue->Push(msg1);
  work_queue->Push(msg2);

  Message exit_msg;
  exit_msg.meta.flag = Flag::kExit;
  work_queue->Push(exit_msg);

  worker_helper_thread.Stop();
  worker_thread1.join();
  worker_thread2.join();
}

}  // namespace flexps

int main() { flexps::TestBgWorker(); }
