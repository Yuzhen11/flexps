#include "gtest/gtest.h"

#include "glog/logging.h"

#include "comm/local_channel.hpp"

namespace flexps {
namespace {

class TestLocalChannel : public testing::Test {
 public:
  TestLocalChannel() {}
  ~TestLocalChannel() {}

 protected:
  void SetUp() {}
  void TearDown() {}
};

class FakeChannel : public AbstractChannel {
 public:
  virtual ~FakeChannel() override {}
  virtual void PushTo(uint32_t id, const SArrayBinStream& bin) {
    bins.push_back({id, bin});
  }
  virtual void Wait() {
    wait_count += 1;
  }

  std::vector<std::pair<uint32_t, SArrayBinStream>> bins;
  int wait_count = 0;
};

TEST_F(TestLocalChannel, Create) {
  FakeChannel fake_channel;
  uint32_t channel_id = 0;
  LocalChannel local_channel(channel_id, &fake_channel);
}

TEST_F(TestLocalChannel, PushTo) {
  FakeChannel fake_channel;
  uint32_t channel_id = 0;
  LocalChannel local_channel(channel_id, &fake_channel);
  SArrayBinStream bin;
  uint32_t to_channel_id = 23;
  int a = 10;
  bin << a;
  local_channel.PushTo(to_channel_id, bin);

  ASSERT_EQ(fake_channel.bins.size(), 1);
  int b;
  EXPECT_EQ(fake_channel.bins[0].first, to_channel_id);
  ASSERT_EQ(fake_channel.bins[0].second.Size(), sizeof(int));
  fake_channel.bins[0].second >> b;
  EXPECT_EQ(a, b);
}

TEST_F(TestLocalChannel, SyncAndGet) {
  FakeChannel fake_channel;
  uint32_t channel_id = 0;
  LocalChannel local_channel(channel_id, &fake_channel);
  ThreadsafeQueue<Message>* queue = local_channel.GetQueue();

  Message msg;
  third_party::SArray<int> s{23};
  msg.AddData(s);
  queue->Push(msg);
  auto bins = local_channel.SyncAndGet();
  ASSERT_EQ(bins.size(), 1);
  ASSERT_EQ(bins[0].Size(), 4);
  int b;
  bins[0] >> b;
  EXPECT_EQ(b, s[0]);
}

}  // namespace
}  // namespace flexps
