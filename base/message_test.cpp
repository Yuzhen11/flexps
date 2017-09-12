#include "glog/logging.h"
#include "gtest/gtest.h"

#include "base/message.hpp"
#include "base/third_party/sarray.h"

namespace flexps {
namespace {

class TestMessage : public testing::Test {
 public:
  TestMessage() {}
  ~TestMessage() {}

 protected:
  void SetUp() {}
  void TearDown() {}
};

TEST_F(TestMessage, BasicNotBreakDown) {
  Message m;
  std::vector<int> keys{1, 2, 3};
  third_party::SArray<int> data(keys);
  m.AddData(data);
}

}  // namespace
}  // namespace flexps