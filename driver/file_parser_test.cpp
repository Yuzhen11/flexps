#include "gtest/gtest.h"

#include "glog/logging.h"

#include "driver/file_parser.hpp"

#include <fstream>
#include <cstdio>

namespace flexps {
namespace {

class TestParseFile: public testing::Test {
 public:
  TestParseFile() {}
  ~TestParseFile() {}

 protected:
  void SetUp() {}
  void TearDown() {}
};

TEST_F(TestParseFile, Parse) {
  std::string filename = "/tmp/file_parser_test-4382018.txt";
  std::ofstream output_file;
  output_file.open(filename);
  output_file << "0:worker1:24345\n";
  output_file << "1:worker1:24346\n";
  output_file.close();

  std::vector<Node> nodes = parse_file(filename);
  EXPECT_EQ(nodes.size(), 2);
  EXPECT_EQ(nodes[0].id, 0);
  EXPECT_EQ(nodes[0].hostname, "worker1");
  EXPECT_EQ(nodes[0].port, 24345);
  EXPECT_EQ(nodes[1].id, 1);
  EXPECT_EQ(nodes[1].hostname, "worker1");
  EXPECT_EQ(nodes[1].port, 24346);

  EXPECT_EQ(remove(filename.c_str()), 0);
}

}  // namespace
}  // namespace flexps
