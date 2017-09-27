#pragma once
#include <map>
#include <string>
#include <unordered_set>
#include <utility>
#include "hdfs/hdfs.h"
#include "master.hpp"
#include "glog/logging.h"
#include "zmq_helper.hpp"
namespace flexps {


struct BlkDesc {
    std::string filename;
    size_t offset;
    std::string block_location;
};

bool operator==(const BlkDesc& lhs, const BlkDesc& rhs);


class HDFSBlockAssigner{
   public: 
    HDFSBlockAssigner(std::string hdfsNameNode, int hdfsNameNodePort);
    ~HDFSBlockAssigner() = default;
    void master_exit_handler_ml();
    void master_main_handler_ml();
    inline bool is_valid() const { return is_valid_; }
    void init_hdfs(const std::string& node, const int& port);
    void browse_hdfs(int id, const std::string& url);
    std::pair<std::string, size_t> answer(const std::string& host, const std::string& url, int id);
    int num_workers_alive;
   
   private:

    bool is_valid_ = false;
    hdfsFS fs_ = NULL;
    std::set<int> finished_workers_;
    std::map<std::string, int> finish_dict;
    std::map<std::string, std::unordered_set<BlkDesc>> files_locality_dict;
    /*
     * files_locality_multi_dict is a map about hdfs info
     * {
     *     {task_id: {{url: {host:[{filename,offset,block_location}, {filename,offset,block_location}...]}},....}},...
     * }
     */
    std::map<size_t, std::map<std::string, std::map<std::string, std::unordered_set<BlkDesc>>>> files_locality_multi_dict;
    /*
     * finish_multi_dict describes each available thread has request the url, and response null
     * if none thread can get anything from an url, this means this url has dispensed all block, this url can be removed
     * {
     *     {task_id:{{url: {{host: count},count_num},...}},...
     * }
     */
    std::map<size_t, std::map<std::string, std::pair<std::map<std::string, size_t>, size_t>>> finish_multi_dict;
};

}

namespace std {
template <>
struct hash<flexps::BlkDesc> {
    size_t operator()(const flexps::BlkDesc& t) const { return hash<string>()(t.filename); }
};
}  // namespace std




