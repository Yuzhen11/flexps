#include "gtest/gtest.h"

#include "glog/logging.h"

#include "driver/node_parser.hpp"

#include <fstream>
#include <cstdio>

namespace flexps {
namespace {

class TestNodeParser: public testing::Test {
 public:
  TestNodeParser() {}
  ~TestNodeParser() {}

 protected:
  void SetUp() {}
  void TearDown() {}
};

TEST_F(TestNodeParser, ParseFile) {
  std::string filename = "/tmp/file_parser_test-4382018.txt";
  std::ofstream output_file;
  output_file.open(filename);
  output_file << "0:worker1:24345\n";
  output_file << "1:worker1:24346\n";
  output_file.close();

  std::vector<Node> nodes = ParseFile(filename);
  EXPECT_EQ(nodes.size(), 2);
  EXPECT_EQ(nodes[0].id, 0);
  EXPECT_EQ(nodes[0].hostname, "worker1");
  EXPECT_EQ(nodes[0].port, 24345);
  EXPECT_EQ(nodes[1].id, 1);
  EXPECT_EQ(nodes[1].hostname, "worker1");
  EXPECT_EQ(nodes[1].port, 24346);

  EXPECT_EQ(remove(filename.c_str()), 0);
}

TEST_F(TestNodeParser, CheckValidNodeIds) {
  std::vector<Node> nodes1{{0, "worker1", 32145}, {1, "worker1", 32141}, {2, "worker2", 45542}};
  EXPECT_EQ(CheckValidNodeIds(nodes1), true);

  std::vector<Node> nodes2{{0, "worker1", 32145}, {1, "worker1", 32141}, {0, "worker2", 45542}};
  EXPECT_EQ(CheckValidNodeIds(nodes2), false);
}

TEST_F(TestNodeParser, CheckUniquePort) {
  std::vector<Node> nodes1{{0, "worker1", 32145}, {1, "worker1", 32141}, {2, "worker1", 45542}};
  EXPECT_EQ(CheckUniquePort(nodes1), true);

  std::vector<Node> nodes2{{0, "worker1", 32145}, {1, "worker1", 32141}, {2, "worker1", 32145}};
  EXPECT_EQ(CheckUniquePort(nodes2), false);
}

TEST_F(TestNodeParser, CheckConsecutiveIds) {
  std::vector<Node> nodes1{{0, "worker1", 32145}, {1, "worker1", 32141}, {2, "worker1", 45542}};
  EXPECT_EQ(CheckConsecutiveIds(nodes1), true);

  std::vector<Node> nodes2{{0, "worker1", 32145}, {2, "worker1", 32141}, {1, "worker1", 45542}};
  EXPECT_EQ(CheckConsecutiveIds(nodes2), false);

  std::vector<Node> nodes3{{1, "worker1", 32145}, {2, "worker1", 32141}, {3, "worker1", 45542}};
  EXPECT_EQ(CheckConsecutiveIds(nodes3), false);
}

}  // namespace
}  // namespace flexps
