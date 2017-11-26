#include "gtest/gtest.h"

#include "glog/logging.h"

#include <thread>

#include "comm/channel.hpp"
#include "comm/fake_mailbox.hpp"


namespace flexps {
namespace {

class TestChannel : public testing::Test {
 public:
  TestChannel() {}
  ~TestChannel() {}

 protected:
  void SetUp() {}
  void TearDown() {}
};

TEST_F(TestChannel, Create) {
  uint32_t num_local_threads = 2;
  uint32_t num_global_threads = 2;
  std::vector<uint32_t> local_thread_ids{0, 1};
  std::unordered_map<uint32_t, uint32_t> id_map{{0, 20}, {1, 21}};
  FakeMailbox mailbox;

  Channel ch(num_local_threads, num_global_threads, local_thread_ids, id_map, &mailbox);
}

TEST_F(TestChannel, PushTo) {
  uint32_t num_local_threads = 2;
  uint32_t num_global_threads = 2;
  std::vector<uint32_t> local_thread_ids{0, 1};
  std::unordered_map<uint32_t, uint32_t> id_map{{0, 20}, {1, 21}};
  FakeMailbox mailbox;

  Channel ch(num_local_threads, num_global_threads, local_thread_ids, id_map, &mailbox);

  SArrayBinStream bin;
  int a = 10;
  bin << a;
  ch.PushTo(1, bin);
  ASSERT_NE(mailbox.queue_map_.find(21), mailbox.queue_map_.end());
  ASSERT_EQ(mailbox.queue_map_[21]->Size(), 1);
  Message msg;
  mailbox.queue_map_[21]->WaitAndPop(&msg);
  SArrayBinStream res;
  res.FromMsg(msg);
  int b;
  res >> b;
  EXPECT_EQ(a, b);
}

TEST_F(TestChannel, Wait) {
  uint32_t num_local_threads = 2;
  uint32_t num_global_threads = 2;
  std::vector<uint32_t> local_thread_ids{0, 1};
  std::unordered_map<uint32_t, uint32_t> id_map{{0, 20}, {1, 21}};
  FakeMailbox mailbox;

  Channel ch(num_local_threads, num_global_threads, local_thread_ids, id_map, &mailbox);
  std::thread th1([&ch](){ ch.Wait(); });
  std::thread th2([&ch](){ ch.Wait(); });
  th1.join();
  th2.join();
}

TEST_F(TestChannel, WaitMultipleTimes) {
  uint32_t num_local_threads = 2;
  uint32_t num_global_threads = 2;
  std::vector<uint32_t> local_thread_ids{0, 1};
  std::unordered_map<uint32_t, uint32_t> id_map{{0, 20}, {1, 21}};
  FakeMailbox mailbox;

  Channel ch(num_local_threads, num_global_threads, local_thread_ids, id_map, &mailbox);
  auto wait = [&ch]() {
    for (int i = 0; i < 100; ++ i) {
      ch.Wait();
    }
  };
  std::thread th1(wait);
  std::thread th2(wait);
  th1.join();
  th2.join();
}

}  // namespace
}  // namespace flexps
