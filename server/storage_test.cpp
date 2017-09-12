#include "glog/logging.h"
#include "gtest/gtest.h"

#include "server/storage.hpp"

namespace flexps {
namespace {

class TestStorage : public testing::Test {
 public:
  TestStorage() {}
  ~TestStorage() {}

 protected:
  void SetUp() {}
  void TearDown() {}
};

TEST_F(TestStorage, Add_Get) {
  Storage<int> s;
  Message m;

  // keys
  std::vector<int> keys{1, 2, 3};
  third_party::SArray<int> s_keys(keys);

  // vals
  std::vector<int> vals{1, 2, 3};
  third_party::SArray<int> s_vals(vals);

  m.AddData(s_keys);
  m.AddData(s_vals);
  s.Add(m);

  Message m2;
  m2.AddData(s_keys);
  Message rep = s.Get(m2);

  EXPECT_EQ(rep.data.size(), 2);
  auto rep_keys = third_party::SArray<int>(rep.data[0]);
  auto rep_vals = third_party::SArray<int>(rep.data[1]);
  for (int index = 0; index < keys.size(); index++) {
    EXPECT_EQ(rep_keys[index], index + 1);
    EXPECT_EQ(rep_vals[index], index + 1);
  }
}

}  // namespace
}  // namespace flexps