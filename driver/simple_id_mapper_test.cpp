#include "gtest/gtest.h"

#include "glog/logging.h"

#include "driver/simple_id_mapper.hpp"

namespace flexps {
namespace {

class TestSimpleIdMapper: public testing::Test {
 public:
  TestSimpleIdMapper() {}
  ~TestSimpleIdMapper() {}

 protected:
  void SetUp() {}
  void TearDown() {}
};

TEST_F(TestSimpleIdMapper, Init) {
  Node n1{0, "worker1", 12352};
  Node n2{1, "worker1", 12353};
  Node n3{3, "worker1", 12354};
  SimpleIdMapper id_mapper(n1, {n1, n2, n3});
  id_mapper.Init(1);
  EXPECT_EQ(id_mapper.GetServerThreadsForId(0).size(), 1);
  EXPECT_EQ(id_mapper.GetServerThreadsForId(0)[0], 0);
  EXPECT_EQ(id_mapper.GetWorkerHelperThreadsForId(0)[0], SimpleIdMapper::kWorkerHelperThreadId);

  EXPECT_EQ(id_mapper.GetServerThreadsForId(1).size(), 1);
  EXPECT_EQ(id_mapper.GetServerThreadsForId(1)[0], SimpleIdMapper::kMaxThreadsPerNode);
  EXPECT_EQ(id_mapper.GetWorkerHelperThreadsForId(1)[0], SimpleIdMapper::kMaxThreadsPerNode + SimpleIdMapper::kWorkerHelperThreadId);

  EXPECT_EQ(id_mapper.GetServerThreadsForId(3).size(), 1);
  EXPECT_EQ(id_mapper.GetServerThreadsForId(3)[0], 3*SimpleIdMapper::kMaxThreadsPerNode);
  EXPECT_EQ(id_mapper.GetWorkerHelperThreadsForId(3)[0], 3*SimpleIdMapper::kMaxThreadsPerNode + SimpleIdMapper::kWorkerHelperThreadId);

  EXPECT_EQ(id_mapper.GetNodeIdForThread(3*SimpleIdMapper::kMaxThreadsPerNode), 3);
  EXPECT_EQ(id_mapper.GetNodeIdForThread(3*SimpleIdMapper::kMaxThreadsPerNode + 1), 3);
  EXPECT_EQ(id_mapper.GetNodeIdForThread(0), 0);
}

TEST_F(TestSimpleIdMapper, AllocateDeallocateThread) {
  Node n1{0, "worker1", 12352};
  Node n2{1, "worker1", 12353};
  Node n3{3, "worker1", 12354};
  SimpleIdMapper id_mapper(n1, {n1, n2, n3});
  EXPECT_EQ(id_mapper.GetWorkerThreadsForId(1).size(), 0);
  id_mapper.Init(1);
  EXPECT_EQ(id_mapper.GetWorkerThreadsForId(1).size(), 0);
  EXPECT_EQ(id_mapper.AllocateWorkerThread(1), SimpleIdMapper::kMaxThreadsPerNode + SimpleIdMapper::kMaxBgThreadsPerNode);
  EXPECT_EQ(id_mapper.GetWorkerThreadsForId(1).size(), 1);
  EXPECT_EQ(id_mapper.GetWorkerThreadsForId(1)[0], SimpleIdMapper::kMaxThreadsPerNode + SimpleIdMapper::kMaxBgThreadsPerNode);
  EXPECT_EQ(id_mapper.AllocateWorkerThread(1), SimpleIdMapper::kMaxThreadsPerNode + SimpleIdMapper::kMaxBgThreadsPerNode + 1);
  EXPECT_EQ(id_mapper.GetWorkerThreadsForId(1).size(), 2);
  EXPECT_EQ(id_mapper.GetWorkerThreadsForId(1)[0], SimpleIdMapper::kMaxThreadsPerNode + SimpleIdMapper::kMaxBgThreadsPerNode);
  EXPECT_EQ(id_mapper.GetWorkerThreadsForId(1)[1], SimpleIdMapper::kMaxThreadsPerNode + SimpleIdMapper::kMaxBgThreadsPerNode + 1);
  id_mapper.DeallocateWorkerThread(1, SimpleIdMapper::kMaxThreadsPerNode + SimpleIdMapper::kMaxBgThreadsPerNode);
  EXPECT_EQ(id_mapper.GetWorkerThreadsForId(1).size(), 1);
  EXPECT_EQ(id_mapper.GetWorkerThreadsForId(1)[0], SimpleIdMapper::kMaxThreadsPerNode + SimpleIdMapper::kMaxBgThreadsPerNode + 1);

  EXPECT_EQ(id_mapper.GetNodeIdForThread(SimpleIdMapper::kMaxThreadsPerNode + SimpleIdMapper::kMaxBgThreadsPerNode), 1);
}

TEST_F(TestSimpleIdMapper, GetChannelThreads) {
  Node n1{0, "worker1", 12352};
  Node n2{1, "worker1", 12353};
  Node n3{3, "worker1", 12354};
  SimpleIdMapper id_mapper(n2, {n1, n2, n3});
  const uint32_t num_local_threads = 2;
  const uint32_t num_global_threads = num_local_threads * 3;
  auto ret = id_mapper.GetChannelThreads(num_local_threads, num_global_threads);
  
  std::pair<std::vector<uint32_t>, std::unordered_map<uint32_t, uint32_t>> expected_ret;
  expected_ret.second.insert({0, SimpleIdMapper::kChannelThreadId});
  expected_ret.second.insert({1, SimpleIdMapper::kChannelThreadId+1});
  expected_ret.second.insert({2, SimpleIdMapper::kMaxThreadsPerNode+SimpleIdMapper::kChannelThreadId});
  expected_ret.second.insert({3, SimpleIdMapper::kMaxThreadsPerNode+SimpleIdMapper::kChannelThreadId+1});
  expected_ret.second.insert({4, 3*SimpleIdMapper::kMaxThreadsPerNode+SimpleIdMapper::kChannelThreadId});
  expected_ret.second.insert({5, 3*SimpleIdMapper::kMaxThreadsPerNode+SimpleIdMapper::kChannelThreadId+1});
  expected_ret.first = {2, 3};
  EXPECT_EQ(ret.first, expected_ret.first);
  ASSERT_EQ(ret.second.size(), expected_ret.second.size());
  for (auto& kv: ret.second) {
    EXPECT_EQ(kv.second, expected_ret.second[kv.first]);
  }
  id_mapper.ReleaseChannelThreads();
}

}  // namespace
}  // namespace flexps
