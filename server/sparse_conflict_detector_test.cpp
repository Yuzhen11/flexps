#include "glog/logging.h"
#include "gtest/gtest.h"

#include "server/sparse_conflict_detector.hpp"

#include <memory>

namespace flexps {
namespace {

class TestSparseConflictDetector : public testing::Test {
 public:
  TestSparseConflictDetector() {}
  ~TestSparseConflictDetector() {}

 protected:
  void SetUp() {}
  void TearDown() {}
};

TEST_F(TestSparseConflictDetector, Constructor) {
  auto detector_ = std::unique_ptr<AbstractConflictDetector>(new SparseConflictDetector());
}

TEST_F(TestSparseConflictDetector, AddAndRemove) {
  auto detector_ = std::unique_ptr<AbstractConflictDetector>(new SparseConflictDetector());

  // thread 0 wanna m0_keys in version 0
  third_party::SArray<uint32_t> m0_keys({0, 1, 2});
  detector_->AddRecord(0, 0, m0_keys);

  // thread 1 wanna m1_keys in version 0
  third_party::SArray<uint32_t> m1_keys({1, 2, 3});
  detector_->AddRecord(0, 1, m1_keys);

  // thread 0 wanna m3_keys in version 1
  third_party::SArray<uint32_t> m2_keys({0, 2, 3});
  detector_->AddRecord(1, 0, m2_keys);

  EXPECT_EQ(dynamic_cast<SparseConflictDetector*>(detector_.get())->ParamSize(0), 4);
  EXPECT_EQ(dynamic_cast<SparseConflictDetector*>(detector_.get())->WorkerSize(0), 2);
  EXPECT_EQ(dynamic_cast<SparseConflictDetector*>(detector_.get())->TotalSize(0), 6);
  EXPECT_EQ(dynamic_cast<SparseConflictDetector*>(detector_.get())->ParamSize(1), 3);
  EXPECT_EQ(dynamic_cast<SparseConflictDetector*>(detector_.get())->WorkerSize(1), 1);
  EXPECT_EQ(dynamic_cast<SparseConflictDetector*>(detector_.get())->TotalSize(1), 3);

  // remove m0_keys record
  detector_->RemoveRecord(0, 0, m0_keys);

  EXPECT_EQ(dynamic_cast<SparseConflictDetector*>(detector_.get())->ParamSize(0), 3);
  EXPECT_EQ(dynamic_cast<SparseConflictDetector*>(detector_.get())->WorkerSize(0), 1);
  EXPECT_EQ(dynamic_cast<SparseConflictDetector*>(detector_.get())->TotalSize(0), 3);

  // Add back
  detector_->AddRecord(0, 0, m0_keys);
  detector_->ClockRemoveRecord(0);

  EXPECT_EQ(dynamic_cast<SparseConflictDetector*>(detector_.get())->ParamSize(0), 0);
  EXPECT_EQ(dynamic_cast<SparseConflictDetector*>(detector_.get())->WorkerSize(0), 0);
  EXPECT_EQ(dynamic_cast<SparseConflictDetector*>(detector_.get())->TotalSize(0), 0);
}

TEST_F(TestSparseConflictDetector, ConflictInfo) {
  auto detector_ = std::unique_ptr<AbstractConflictDetector>(new SparseConflictDetector());

  // thread 1 wanna 1, 2, 3 in version 0
  third_party::SArray<uint32_t> m1_keys({1, 2, 3});
  detector_->AddRecord(0, 1, m1_keys);

  // thread 1 wanna 0, 3, 4 in version 1
  third_party::SArray<uint32_t> m2_keys({0, 3, 4});
  detector_->AddRecord(1, 1, m2_keys);

  // thread 2 wanna 0, 2, 3 in version 0
  third_party::SArray<uint32_t> m3_keys({0, 1, 2});
  detector_->AddRecord(0, 2, m3_keys);

  // thread 2 wanna 5 in version 1
  third_party::SArray<uint32_t> m4_keys({5});
  detector_->AddRecord(1, 2, m4_keys);

  // thread 3 wanna 2, 5 in version 0
  third_party::SArray<uint32_t> m5_keys({2, 5});
  detector_->AddRecord(0, 3, m5_keys);

  int forwarded_thread_id = -1;
  int forwarded_version = -1;
  bool check_flag = false;

  third_party::SArray<uint32_t> check_keys({5});
  check_flag = detector_->ConflictInfo(check_keys, 0, 1, forwarded_thread_id, forwarded_version);

  EXPECT_EQ(check_flag, true);
  EXPECT_EQ(forwarded_thread_id, 2);
  EXPECT_EQ(forwarded_version, 1);

  detector_->RemoveRecord(1, 2, m4_keys);
  check_flag = detector_->ConflictInfo(check_keys, 0, 1, forwarded_thread_id, forwarded_version);

  EXPECT_EQ(check_flag, true);
  EXPECT_EQ(forwarded_thread_id, 3);
  EXPECT_EQ(forwarded_version, 0);

  detector_->RemoveRecord(0, 3, m5_keys);
  check_flag = detector_->ConflictInfo(check_keys, 0, 1, forwarded_thread_id, forwarded_version);

  EXPECT_EQ(check_flag, false);
}

}  // namespace
}  // namespace flexps
