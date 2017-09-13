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
  id_mapper.Init();
  EXPECT_EQ(id_mapper.GetServerThreadsForId(0).size(), 1);
  EXPECT_EQ(id_mapper.GetServerThreadsForId(0)[0], 0);
  EXPECT_EQ(id_mapper.GetWorkerHelperThreadsForId(0)[0], 1);

  EXPECT_EQ(id_mapper.GetServerThreadsForId(1).size(), 1);
  EXPECT_EQ(id_mapper.GetServerThreadsForId(1)[0], SimpleIdMapper::kMaxThreadsPerNode);
  EXPECT_EQ(id_mapper.GetWorkerHelperThreadsForId(1)[0], SimpleIdMapper::kMaxThreadsPerNode + 1);

  EXPECT_EQ(id_mapper.GetServerThreadsForId(3).size(), 1);
  EXPECT_EQ(id_mapper.GetServerThreadsForId(3)[0], 3*SimpleIdMapper::kMaxThreadsPerNode);
  EXPECT_EQ(id_mapper.GetWorkerHelperThreadsForId(3)[0], 3*SimpleIdMapper::kMaxThreadsPerNode + 1);

  EXPECT_EQ(id_mapper.GetNodeIdForThreads(3*SimpleIdMapper::kMaxThreadsPerNode), 3);
  EXPECT_EQ(id_mapper.GetNodeIdForThreads(3*SimpleIdMapper::kMaxThreadsPerNode + 1), 3);
  EXPECT_EQ(id_mapper.GetNodeIdForThreads(0), 0);
}

TEST_F(TestSimpleIdMapper, AllocateDeallocateThread) {
  Node n1{0, "worker1", 12352};
  Node n2{1, "worker1", 12353};
  Node n3{3, "worker1", 12354};
  SimpleIdMapper id_mapper(n1, {n1, n2, n3});
  EXPECT_EQ(id_mapper.GetWorkerThreadsForId(1).size(), 0);
  id_mapper.Init();
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

  EXPECT_EQ(id_mapper.GetNodeIdForThreads(SimpleIdMapper::kMaxThreadsPerNode + SimpleIdMapper::kMaxBgThreadsPerNode), 1);
}

}  // namespace
}  // namespace flexps
