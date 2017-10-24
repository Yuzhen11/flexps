#pragma once

#include <string>
#include "boost/utility/string_ref.hpp"
#include "io/coordinator.hpp"
#include "glog/logging.h"
#include "hdfs/hdfs.h"

namespace flexps {

class HDFSFileSplitter {
 public:
  HDFSFileSplitter(int num_threads, int id, Coordinator* coordinator, std::string hostname, std::string hdfs_namenode,
                   int hdfs_namenode_port);
  ~HDFSFileSplitter();
  boost::string_ref fetch_block(bool is_next);
  void load(const std::string& url);
  inline size_t get_offset() { return offset_; }
 private:
  void init_blocksize(hdfsFS fs, const std::string& url);
  int read_block(const std::string& fn);
  Coordinator* coordinator_;
  int num_threads_;
  int id_;
  size_t offset_ = 0;
  // using heap
  char* data_;
  hdfsFile file_ = NULL;
  hdfsFS fs_;
  std::string url_;
  std::string hostname_;
  std::string hdfs_namenode_;
  int hdfs_namenode_port_;
  size_t hdfs_block_size;
};

}  // namespace flexps
