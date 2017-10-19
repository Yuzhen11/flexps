#include "glog/logging.h"
#include "gtest/gtest.h"

#include "server/min_counting.hpp"

namespace flexps {
namespace {

class TestMinCounting : public testing::Test {
 public:
  TestMinCounting() {}
  ~TestMinCounting() {}

 protected:
  void SetUp() {}
  void TearDown() {}
};

TEST_F(TestMinCounting, Construtor) {
  MinCounting recorder(3, 100);
}

TEST_F(TestMinCounting, HashFuncResultSame) {
  const int num_hash = 3;
  const int num_bin = 100;

  MinCounting recorder(num_hash, num_bin);
  std::vector<uint32_t> random_keys{21321, 213, 63324, 624241};

  for (auto& key : random_keys) {
    // first round
    std::vector<uint32_t> first_output(num_hash);
    recorder.HashFuncResult(key, first_output);

    // second round
    std::vector<uint32_t> second_output(num_hash);
    recorder.HashFuncResult(key, second_output);

    EXPECT_EQ(first_output.size(), second_output.size());
    for (int hash_index = 0; hash_index < num_hash; hash_index ++) {
      EXPECT_EQ(first_output[hash_index], second_output[hash_index]);
    }
  }
}

TEST_F(TestMinCounting, Timer) {
  const int num_hash = 3;
  const int num_bin = 100;

  MinCounting recorder(num_hash, num_bin);
  recorder.HashFuncTimer(10000);
}

TEST_F(TestMinCounting, AddRemoveFind) {
  // Set small cuz otherwise so troublesome
  const int num_hash = 3;
  const int num_bin = 10;

  MinCounting recorder(num_hash, num_bin);
  
  third_party::SArray<uint32_t> add_keys({1, 2, 2, 3, 4, 2, 3, 5, 1, 6, 7, 4, 7, 5, 7, 3241, 134, 454});
  recorder.Add(add_keys);
  EXPECT_GE(recorder.Count(1), 2);
  EXPECT_GE(recorder.Count(2), 3);

  third_party::SArray<uint32_t> remove_keys({2});
  recorder.Remove(remove_keys);
  EXPECT_GE(recorder.Count(2), 2);

  recorder.Remove(remove_keys);
  recorder.Remove(remove_keys);
  EXPECT_GE(recorder.Count(2), 0);
}

}  // namespace
}  // namespace flexps
