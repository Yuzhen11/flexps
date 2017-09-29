#include "hdfs_assigner.hpp"
#include "base/serialization.hpp"
#include "glog/logging.h"
#include "hdfs/hdfs.h"
#include "master.hpp"
#include "zmq_helper.hpp"

namespace flexps {

bool operator==(const BlkDesc& lhs, const BlkDesc& rhs) {
  return lhs.filename == rhs.filename && lhs.offset == rhs.offset && lhs.block_location == rhs.block_location;
}

HDFSBlockAssigner::HDFSBlockAssigner(std::string hdfs_namenode, int hdfs_namenode_port) {
  // 301 is a constant for IO load
  // 300 is for exit procedure
  Master::get_instance().register_main_handler(301, std::bind(&HDFSBlockAssigner::master_main_handler_ml, this));
  Master::get_instance().register_main_handler(300, std::bind(&HDFSBlockAssigner::master_exit_handler_ml, this));
  init_hdfs(hdfs_namenode, hdfs_namenode_port);
}

void HDFSBlockAssigner::master_exit_handler_ml() {
  auto& master = Master::get_instance();
  auto master_handler = master.get_socket();
  std::string worker_name;
  int worker_id;
  BinStream stream;
  zmq::message_t msg;
  zmq_recv_common(master_handler.get(), &msg);
  stream.push_back_bytes(reinterpret_cast<char*>(msg.data()), msg.size());
  stream >> worker_name >> worker_id;
  finished_workers_.insert(worker_id);

  LOG(INFO) << "master => worker finsished @" << worker_name << "-" << std::to_string(worker_id);

  if ((finished_workers_.size() == num_workers_alive)) {
    master.halt();
  }
}

void HDFSBlockAssigner::master_main_handler_ml() {
  auto& master = Master::get_instance();
  auto master_socket = master.get_socket();
  std::string url, host, load_type;
  int num_threads, id;

  zmq::message_t msg1;
  zmq_recv_common(master_socket.get(), &msg1);
  BinStream stream;
  stream.push_back_bytes(reinterpret_cast<char*>(msg1.data()), msg1.size());
  stream >> url >> host >> num_threads >> id;

  // reset num_worker_alive
  num_workers_alive = num_threads;
  LOG(INFO) << url << host << num_threads << id << load_type;
  std::pair<std::string, size_t> ret = answer(host, url, id);
  stream.clear();
  stream << ret.first << ret.second;

  zmq_send_common(master_socket.get(), master.get_cur_client().data(), master.get_cur_client().length(), ZMQ_SNDMORE);
  zmq_send_common(master_socket.get(), nullptr, 0, ZMQ_SNDMORE);
  zmq_send_common(master_socket.get(), stream.get_remained_buffer(), stream.size());
}

void HDFSBlockAssigner::init_hdfs(const std::string& node, const int& port) {
  int num_retries = 3;
  while (num_retries--) {
    struct hdfsBuilder* builder = hdfsNewBuilder();
    hdfsBuilderSetNameNode(builder, node.c_str());
    hdfsBuilderSetNameNodePort(builder, port);
    fs_ = hdfsBuilderConnect(builder);
    hdfsFreeBuilder(builder);
    if (fs_)
      break;
  }
  if (fs_) {
    is_valid_ = true;
    return;
  }
  LOG(INFO) << "Failed to connect to HDFS " << node << ":" << port;
}

void HDFSBlockAssigner::browse_hdfs(int id, const std::string& url) {
  if (!fs_)
    return;

  int num_files;
  int dummy;
  hdfsFileInfo* file_info = hdfsListDirectory(fs_, url.c_str(), &num_files);
  auto& files_locality = files_locality_multi_dict[id][url];
  for (int i = 0; i < num_files; ++i) {
    // for every file in a directory
    if (file_info[i].mKind != kObjectKindFile)
      continue;
    size_t k = 0;
    while (k < file_info[i].mSize) {
      // for every block in a file
      auto blk_loc = hdfsGetFileBlockLocations(fs_, file_info[i].mName, k, 1, &dummy);
      for (int j = 0; j < blk_loc->numOfNodes; ++j) {
        // for every replication in a block
        files_locality[blk_loc->hosts[j]].insert(
            BlkDesc{std::string(file_info[i].mName) + '\0', k, std::string(blk_loc->hosts[j])});
        // init finish_multi_dict, 0 means none thread has been rejected by this (url, host)
        (finish_multi_dict[id][url].first)[blk_loc->hosts[j]] = 0;
      }
      k += file_info[i].mBlockSize;
    }
  }
  hdfsFreeFileInfo(file_info, num_files);
}

std::pair<std::string, size_t> HDFSBlockAssigner::answer(const std::string& host, const std::string& url, int id) {
  if (!fs_)
    return {"", 0};

  // cannot find id
  if (files_locality_multi_dict.find(id) == files_locality_multi_dict.end()) {
    browse_hdfs(id, url);
    // count the url accessing times
    finish_multi_dict[id][url].second = 0;
  }

  // cannot find url
  if (files_locality_multi_dict[id].find(url) == files_locality_multi_dict[id].end()) {
    browse_hdfs(id, url);
    finish_multi_dict[id][url].second = 0;
  }

  // selected_file, offset
  std::pair<std::string, size_t> ret = {"", 0};

  auto& all_files_locality = files_locality_multi_dict[id][url];
  /*
   * selected_host has two situation:
   * 1. when load_locally, selected_host always equals host, or null
   * 2. when load_globally,
   *     when loading local host, selected_host equals host,
   *     when loading gloabl host, selected_host equals an unfinished host
   */
  std::string selected_host;
  //      if (load_type.empty() || load_type == "load_hdfs_globally") { // default is load data globally
  // selected_file
  // if there is local file, allocate local file
  if (all_files_locality[host].size()) {
    selected_host = host;
  } else {  // there is no local file, so need to check all hosts
            // when loading data globally, util that all hosts finished means finishing
    bool is_finish = true;
    for (auto& item : all_files_locality) {
      // when loading globally, any host havingn't been finished means unfinished
      if (item.second.size() != 0) {
        is_finish = false;
        // in fact, there can be optimizing. for example, everytime, we can calculate the host
        // which has the longest blocks. It may be good for load balance but search is time-consuming
        selected_host = item.first;  // default allocate a unfinished host block
        break;
      }
    }

    if (is_finish) {
      finish_multi_dict[id][url].second += 1;
      if (finish_multi_dict[id][url].second == num_workers_alive) {
        // this means all workers's requests about this url are rejected
        // blocks under this url are all allocated
        files_locality_multi_dict[id].erase(url);
      }
      return {"", 0};
    }
  }

  // according selected_host to get the select file
  auto selected_file = all_files_locality[selected_host].begin();
  // select
  ret = {selected_file->filename, selected_file->offset};

  // cautious: need to remove all replicas in different host
  for (auto its = all_files_locality.begin(); its != all_files_locality.end(); its++) {
    for (auto it = its->second.begin(); it != its->second.end(); it++) {
      if (it->filename == ret.first && it->offset == ret.second) {
        all_files_locality[its->first].erase(it);
        break;
      }
    }
  }

  return ret;
}
}  // namespace flexps
