#include "gtest/gtest.h"

#include "glog/logging.h"

#include "worker/kv_client_table.hpp"

namespace flexps {
namespace {

class TestKVClientTable : public testing::Test {
 public:
  TestKVClientTable() {}
  ~TestKVClientTable() {}

 protected:
  void SetUp() {}
  void TearDown() {}
};

class FakeRangeManager : public AbstractRangeManager {
 public:
  FakeRangeManager(const std::vector<third_party::Range>& ranges)
    : ranges_(ranges) {}
  virtual size_t GetNumServers() const override {
    return ranges_.size();
  }
  virtual const std::vector<third_party::Range>& GetRanges() const override {
    return ranges_;
  }
 private:
  std::vector<third_party::Range> ranges_;
};

TEST_F(TestKVClientTable, Init) {
  ThreadsafeQueue<Message> queue;
  FakeRangeManager manager({{2, 4}, {4, 7}});
  KVClientTable<float> table(0, 0, &queue, &manager);
}

}  // namespace
}  // namespace flexps
