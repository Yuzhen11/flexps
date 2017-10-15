#pragma once

#include <map>
#include <memory>
#include <set>
#include <string>
#include <unordered_set>
#include <utility>
#include "hdfs/hdfs.h"
#include "zmq.hpp"

namespace flexps {

class HDFSBlockAssigner {
 public:
  // 301 is a constant for IO load
  // 300 is for exit procedure
  static const int kBlockRequest = 301;
  static const int kExit = 300;

  struct BlkDesc {
    std::string filename;
    size_t offset;
    std::string block_location;
    bool operator==(const BlkDesc& other) const;
  };

  HDFSBlockAssigner(std::string hdfsNameNode, int hdfsNameNodePort, zmq::context_t* context, int master_port);
  ~HDFSBlockAssigner() = default;

  void Serve();

 private:
  void halt();
  void handle_exit();
  void handle_block_request(const std::string& cur_client);
  void init_socket(int master_port, zmq::context_t* zmq_context);
  void init_hdfs(const std::string& node, const int& port);
  void browse_hdfs(int id, const std::string& url);
  std::pair<std::string, size_t> answer(const std::string& host, const std::string& url, int id);

 private:
  bool running_ = true;
  std::unique_ptr<zmq::socket_t> master_socket_;

  hdfsFS fs_ = NULL;
  std::set<int> finished_workers_;
  int num_workers_alive_;
  std::map<std::string, int> finish_dict;
  std::map<std::string, std::unordered_set<BlkDesc>> files_locality_dict;

  // {task_id: {{url: {host:[{filename,offset,block_location}, {filename,offset,block_location}...]}},....}},...
  std::map<size_t, std::map<std::string, std::map<std::string, std::unordered_set<BlkDesc>>>>
      files_locality_multi_dict_;

  // finish_multi_dict_ describes each available thread has request the url, and response null
  // if none thread can get anything from an url, this means this url has dispensed all block, this url can be removed
  // {task_id:{{url: {{host: count},count_num},...}},...
  std::map<size_t, std::map<std::string, std::pair<std::map<std::string, size_t>, size_t>>> finish_multi_dict_;
};

}  // namespace flexps

namespace std {
template <>
struct hash<flexps::HDFSBlockAssigner::BlkDesc> {
  size_t operator()(const flexps::HDFSBlockAssigner::BlkDesc& t) const { return hash<string>()(t.filename); }
};
}  // namespace std
