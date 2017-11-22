#include "gtest/gtest.h"

#include "glog/logging.h"

#include <thread>

#include "comm/channel.hpp"
#include "comm/mailbox.hpp"  // Depends on the real mailbox
#include "comm/local_channel.hpp"  // Some parts also depend on LocalChannel


// This test depends on the real mailbox

namespace flexps {
namespace {

class FakeIdMapper : public AbstractIdMapper {
 public:
  virtual uint32_t GetNodeIdForThread(uint32_t tid) override {
    return tid / 10;
  }
};

class TestChannelIntegration : public testing::Test {
 public:
  TestChannelIntegration() {}
  ~TestChannelIntegration() {}

 protected:
  void SetUp() {}
  void TearDown() {}
};

TEST_F(TestChannelIntegration, Wait) {
  std::vector<Node> nodes{
    {0, "localhost", 12353},
    {1, "localhost", 12354},
    {2, "localhost", 12355}};
  uint32_t num_local_threads = 2;
  uint32_t num_global_threads = 6;  // 3 nodes each with 2 threads, 2*3 = 6
  std::vector<std::vector<uint32_t>> local_thread_ids{{0, 1}, {2, 3}, {4, 5}};
  std::unordered_map<uint32_t, uint32_t> id_map{{0, 5}, {1, 6}, {2, 15}, {3, 16}, {4, 25}, {5, 26}};

  std::vector<std::thread> threads(nodes.size());
  for (int i = 0; i < nodes.size(); ++ i) {
    threads[i] = std::thread([=]() {
      FakeIdMapper id_mapper;
      Mailbox mailbox(nodes[i], nodes, &id_mapper);
      mailbox.Start();
      Channel ch(num_local_threads, num_global_threads, local_thread_ids[i], id_map, &mailbox);
      std::thread th1([&ch](){ ch.Wait(); });
      std::thread th2([&ch](){ ch.Wait(); });
      th1.join();
      th2.join();
      mailbox.Stop();
    });
  }
  for (auto& th : threads) {
    th.join();
  }
}

TEST_F(TestChannelIntegration, WaitMultipleTimes) {
  std::vector<Node> nodes{
    {0, "localhost", 12353},
    {1, "localhost", 12354},
    {2, "localhost", 12355}};
  uint32_t num_local_threads = 2;
  uint32_t num_global_threads = 6;  // 3 nodes each with 2 threads, 2*3 = 6
  std::vector<std::vector<uint32_t>> local_thread_ids{{0, 1}, {2, 3}, {4, 5}};
  std::unordered_map<uint32_t, uint32_t> id_map{{0, 5}, {1, 6}, {2, 15}, {3, 16}, {4, 25}, {5, 26}};

  std::vector<std::thread> threads(nodes.size());
  for (int i = 0; i < nodes.size(); ++ i) {
    threads[i] = std::thread([=]() {
      FakeIdMapper id_mapper;
      Mailbox mailbox(nodes[i], nodes, &id_mapper);
      mailbox.Start();
      Channel ch(num_local_threads, num_global_threads, local_thread_ids[i], id_map, &mailbox);
      auto wait = [&ch]() {
        for (int i = 0; i < 100; ++ i) {
          ch.Wait();
        }
      };
      std::thread th1(wait);
      std::thread th2(wait);
      th1.join();
      th2.join();
      mailbox.Stop();
    });
  }
  for (auto& th : threads) {
    th.join();
  }
}

TEST_F(TestChannelIntegration, SyncAndGetEmpty) {
  std::vector<Node> nodes{
    {0, "localhost", 12353},
    {1, "localhost", 12354}};
  uint32_t num_local_threads = 1;
  uint32_t num_global_threads = 2;  // 2 nodes each with 1 thread, 2*1 = 2 
  std::vector<std::vector<uint32_t>> local_thread_ids{{0}, {1}};
  std::unordered_map<uint32_t, uint32_t> id_map{{0, 5}, {1, 15}};

  std::vector<std::thread> threads(nodes.size());
  for (int i = 0; i < nodes.size(); ++ i) {
    threads[i] = std::thread([=]() {
      FakeIdMapper id_mapper;
      Mailbox mailbox(nodes[i], nodes, &id_mapper);
      mailbox.Start();
      Channel ch(num_local_threads, num_global_threads, local_thread_ids[i], id_map, &mailbox);
      auto local_channels = ch.GetLocalChannels();
      ASSERT_EQ(local_channels.size(), 1);
      auto* local_channel = local_channels[0];
      auto bins = local_channel->SyncAndGet();
      EXPECT_EQ(bins.size(), 0);
      mailbox.Stop();
    });
  }
  for (auto& th : threads) {
    th.join();
  }
}

TEST_F(TestChannelIntegration, SyncAndGetSome) {
  std::vector<Node> nodes{
    {0, "localhost", 12353},
    {1, "localhost", 12354}};
  uint32_t num_local_threads = 1;
  uint32_t num_global_threads = 2;  // 2 nodes each with 1 thread, 2*1 = 2 
  std::vector<std::vector<uint32_t>> local_thread_ids{{0}, {1}};
  std::unordered_map<uint32_t, uint32_t> id_map{{0, 5}, {1, 15}};

  std::vector<std::thread> threads(nodes.size());
  for (int i = 0; i < nodes.size(); ++ i) {
    threads[i] = std::thread([=]() {
      FakeIdMapper id_mapper;
      Mailbox mailbox(nodes[i], nodes, &id_mapper);
      mailbox.Start();
      Channel ch(num_local_threads, num_global_threads, local_thread_ids[i], id_map, &mailbox);
      auto local_channels = ch.GetLocalChannels();
      ASSERT_EQ(local_channels.size(), 1);
      auto* local_channel = local_channels[0];
      SArrayBinStream bin;
      bin << i;
      local_channel->PushTo(1-i, bin);  // each local_channel pushes to the other
      auto bins = local_channel->SyncAndGet();
      ASSERT_EQ(bins.size(), 1);
      int res;
      bins[0] >> res;
      EXPECT_EQ(res, 1-i);
      mailbox.Stop();
    });
  }
  for (auto& th : threads) {
    th.join();
  }
}

TEST_F(TestChannelIntegration, PushToSyncAndGet) {
  std::vector<Node> nodes{
    {0, "localhost", 12353},
    {1, "localhost", 12354},
    {2, "localhost", 12355}};
  uint32_t num_local_threads = 2;
  uint32_t num_global_threads = 6;  // 3 nodes each with 2 threads, 2*3 = 6
  std::vector<std::vector<uint32_t>> local_thread_ids{{0, 1}, {2, 3}, {4, 5}};
  std::unordered_map<uint32_t, uint32_t> id_map{{0, 5}, {1, 6}, {2, 15}, {3, 16}, {4, 25}, {5, 26}};

  std::vector<std::thread> threads(nodes.size());
  for (int i = 0; i < nodes.size(); ++ i) {
    threads[i] = std::thread([=]() {
      FakeIdMapper id_mapper;
      Mailbox mailbox(nodes[i], nodes, &id_mapper);
      mailbox.Start();
      Channel ch(num_local_threads, num_global_threads, local_thread_ids[i], id_map, &mailbox);
      auto local_channels = ch.GetLocalChannels();
      ASSERT_EQ(local_channels.size(), 2);

      auto local_thread = [&ch, num_global_threads](LocalChannel* lc) {
        for (int i = 0; i < 10; ++ i) {
          LOG(INFO) << "Iter: " << i;
          SArrayBinStream bin;
          bin << lc->GetId();
          uint32_t to = (lc->GetId()+1)%num_global_threads;
          uint32_t from = (lc->GetId()+num_global_threads-1)%num_global_threads;
          lc->PushTo(to, bin);
          auto bins = lc->SyncAndGet();
          ASSERT_EQ(bins.size(), 1);
          int res;
          bins[0] >> res;
          EXPECT_EQ(res, from);
        }
      };
      std::thread th1([&]() { local_thread(local_channels[0]); });
      std::thread th2([&]() { local_thread(local_channels[1]); });
      th1.join();
      th2.join();
      mailbox.Stop();
    });
  }
  for (auto& th : threads) {
    th.join();
  }
}

}  // namespace
}  // namespace flexps
