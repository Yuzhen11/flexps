#include "glog/logging.h"
#include "gtest/gtest.h"

#include "server/vector_storage.hpp"

namespace flexps {
namespace {

class TestVectorStorage : public testing::Test {
 public:
  TestVectorStorage() {}
  ~TestVectorStorage() {}

 protected:
  void SetUp() {}
  void TearDown() {}
};

TEST_F(TestVectorStorage, Create) {
  VectorStorage<int> s({10, 20});
  VectorStorage<float> s1({10, 20});
  VectorStorage<double> s2({10, 20});
}

TEST_F(TestVectorStorage, Size) {
  VectorStorage<int> s({10, 20});
  EXPECT_EQ(s.Size(), 10);
  VectorStorage<float> s1({10, 21});
  EXPECT_EQ(s1.Size(), 11);
  VectorStorage<double> s2({10, 22});
  EXPECT_EQ(s2.Size(), 12);
}

TEST_F(TestVectorStorage, AddGetInt) {
  VectorStorage<int> s({10, 20});

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

TEST_F(TestVectorStorage, AddGetFloat) {
  VectorStorage<float> s({10, 20});

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

TEST_F(TestVectorStorage, SubAddSubGet) {
  VectorStorage<float> s({10, 20});

  third_party::SArray<Key> s_keys({13, 14, 15});
  third_party::SArray<float> s_vals({0.1, 0.2, 0.3});
  s.SubAdd(s_keys, third_party::SArray<char>(s_vals));
  third_party::SArray<float> ret = third_party::SArray<float>(s.SubGet(s_keys));
  for (int i = 0; i < s_keys.size(); ++ i) {
    EXPECT_EQ(ret[i], s_vals[i]);
  }
}

TEST_F(TestVectorStorage, SubAddChunkSubGetChunk) {
  VectorStorage<float> s({10, 40}, 10);
  third_party::SArray<float> s_vals(20);
  third_party::SArray<Key> s_keys({1, 2});
  for (int i = 0; i < 20; i++) {
    s_vals[i] = i / 10.0;
  }
  s.SubAddChunk(s_keys, third_party::SArray<char>(s_vals));
  third_party::SArray<float> ret = third_party::SArray<float>(s.SubGetChunk(s_keys));
  for (int i = 0; i < s_vals.size(); ++ i) {
    EXPECT_EQ(ret[i], s_vals[i]);
  }
}


}  // namespace
}  // namespace flexps

