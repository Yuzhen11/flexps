#include "glog/logging.h"
#include "gtest/gtest.h"

#include "server/progress_tracker.hpp"

namespace flexps {
namespace {

class TestProgressTracker : public testing::Test {
 public:
  TestProgressTracker() {}
  ~TestProgressTracker() {}

 protected:
  void SetUp() {}
  void TearDown() {}
};

TEST_F(TestProgressTracker, Basic) {
  ProgressTracker tracker;
  tracker.Init({2, 7});
  EXPECT_EQ(tracker.GetNumThreads(), 2);
  EXPECT_EQ(tracker.GetProgress(2), 0);
  EXPECT_EQ(tracker.GetProgress(7), 0);
}

TEST_F(TestProgressTracker, CheckThreadValid) {
  ProgressTracker tracker;
  tracker.Init({2, 7});
  EXPECT_TRUE(tracker.CheckThreadValid(2));
  EXPECT_FALSE(tracker.CheckThreadValid(3));
  EXPECT_FALSE(tracker.CheckThreadValid(6));
  EXPECT_TRUE(tracker.CheckThreadValid(7));
}

TEST_F(TestProgressTracker, Advance) {
  ProgressTracker tracker;
  tracker.Init({2, 7});
  EXPECT_EQ(tracker.GetMinClock(), 0);
  EXPECT_EQ(tracker.AdvanceAndGetChangedMinClock(2), -1);  // [1,0]
  EXPECT_EQ(tracker.AdvanceAndGetChangedMinClock(7), 1);   // [1,1]
  EXPECT_EQ(tracker.AdvanceAndGetChangedMinClock(7), -1);  // [1,2]
  EXPECT_EQ(tracker.AdvanceAndGetChangedMinClock(7), -1);  // [1,3]
  EXPECT_EQ(tracker.AdvanceAndGetChangedMinClock(2), 2);   // [2,3]
  EXPECT_EQ(tracker.GetProgress(2), 2);
  EXPECT_EQ(tracker.GetProgress(7), 3);
}

}  // namespace
}  // namespace flexps
