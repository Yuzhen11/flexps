#include "glog/logging.h"
#include "gtest/gtest.h"

#include "server/map_storage.hpp"

namespace flexps {
namespace {

class TestMapStorage : public testing::Test {
 public:
  TestMapStorage() {}
  ~TestMapStorage() {}

 protected:
  void SetUp() {}
  void TearDown() {}
};

TEST_F(TestMapStorage, AddGetInt) {
  MapStorage<int> s;

  Message m;
  third_party::SArray<Key> s_keys({13, 14, 15});
  third_party::SArray<int> s_vals({1, 2, 3});
  m.AddData(s_keys);
  m.AddData(s_vals);
  s.Add(m);

  Message m2;
  m2.AddData(s_keys);
  Message rep = s.Get(m2);

  EXPECT_EQ(rep.data.size(), 2);
  auto rep_keys = third_party::SArray<Key>(rep.data[0]);
  auto rep_vals = third_party::SArray<int>(rep.data[1]);
  for (int index = 0; index < s_keys.size(); index++) {
    EXPECT_EQ(rep_keys[index], s_keys[index]);
    EXPECT_EQ(rep_vals[index], s_vals[index]);
  }
}

TEST_F(TestMapStorage, AddGetFloat) {
  MapStorage<float> s;

  Message m;
  third_party::SArray<Key> s_keys({13, 14, 15});
  third_party::SArray<float> s_vals({0.1, 0.2, 0.3});
  m.AddData(s_keys);
  m.AddData(s_vals);
  s.Add(m);

  Message m2;
  m2.AddData(s_keys);
  Message rep = s.Get(m2);

  EXPECT_EQ(rep.data.size(), 2);
  auto rep_keys = third_party::SArray<Key>(rep.data[0]);
  auto rep_vals = third_party::SArray<float>(rep.data[1]);
  for (int index = 0; index < s_keys.size(); index++) {
    EXPECT_EQ(rep_keys[index], s_keys[index]);
    EXPECT_EQ(rep_vals[index], s_vals[index]);
  }
}

TEST_F(TestMapStorage, SubAddSubGet) {
  MapStorage<float> s;

  third_party::SArray<Key> s_keys({13, 14, 15});
  third_party::SArray<float> s_vals({0.1, 0.2, 0.3});
  s.SubAdd(s_keys, third_party::SArray<char>(s_vals));
  third_party::SArray<float> ret = third_party::SArray<float>(s.SubGet(s_keys));
  for (int i = 0; i < s_keys.size(); ++ i) {
    EXPECT_EQ(ret[i], s_vals[i]);
  }
}

TEST_F(TestMapStorage, ChunkBasedSubAddSubGet) {
  uint32_t chunk_size = 2;
  MapStorage<float> s(chunk_size);

  third_party::SArray<Key> s_keys({13, 14, 15});
  third_party::SArray<float> s_vals({0.1, 0.2, 0.3, 0.4, 0.5, 0.6});
  s.SubAdd(s_keys, third_party::SArray<char>(s_vals));
  third_party::SArray<float> ret = third_party::SArray<float>(s.SubGet(s_keys));
  for (int i = 0; i < s_keys.size() * chunk_size; ++ i) {
    EXPECT_EQ(ret[i], s_vals[i]);
  }
}
}  // namespace
}  // namespace flexps
