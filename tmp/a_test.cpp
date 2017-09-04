#include "gtest/gtest.h"

#include "a.hpp"

namespace {

class TestA : public testing::Test {
 public:
  TestA() {}
  ~TestA() {}

 protected:
  void SetUp() {}
  void TearDown() {}
};

TEST_F(TestA, Get) {
  A a;
  EXPECT_EQ(a.Get(), 10);
}

}  // namespace
