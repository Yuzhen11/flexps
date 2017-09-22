#pragma once

#include "hdfs/hdfs.h"
#include "master.h"
#include "glog/logging.h"

namespace flexps {


struct BlkDesc {
    std::string filename;
    size_t offset;
    std::string block_location;
};

bool operator==(const BlkDesc& lhs, const BlkDesc& rhs);


class HDFSBlockAssigner{
   public: 
    HDFSBlockAssigner();
    ~HDFSBlockAssigner() = default;
    void master_main_handler_ml();
    inline bool is_valid() const { return is_valid_; }
    void init_hdfs(const std::string& node, const std::string& port);
    void browse_hdfs(const std::string& url);
    std::pair<std::string, size_t> answer(const std::string& host, const std::string& url);

    int num_workers_alive;
   
   private:
    inline void zmq_send_common(zmq::socket_t* socket, const void* data, const size_t& len, int flag = !ZMQ_NOBLOCK) {
        CHECK(socket != nullptr) << "zmq::socket_t cannot be nullptr!";
        CHECK(data != nullptr || len == 0) << "data and len are not matched!";
        while (true)
            try {
                size_t bytes = socket->send(data, len, flag);
                CHECK(bytes == len) << "zmq::send error!";
                break;
            } catch (zmq::error_t e) {
            switch (e.num()) {
                case EHOSTUNREACH:
                case EINTR:
                continue;
            default:
                LOG(ERROR) << "Invalid type of zmq::error!";
            }
        }
    }

    inline void zmq_recv_common(zmq::socket_t* socket, zmq::message_t* msg, int flag = !ZMQ_NOBLOCK) {
        CHECK(socket != nullptr) << "zmq::socket_t cannot be nullptr!";
        CHECK(msg != nullptr) << "data and len are not matched!";
        while (true)
            try {
                bool successful = socket->recv(msg, flag);
                CHECK(successful) << "zmq::receive error!";
                break;
            } catch (zmq::error_t e) {
            if (e.num() == EINTR)
                continue;
            LOG(ERROR) << "Invalid type of zmq::error!";
        }
    }

    bool is_valid_ = false;
    hdfsFS fs_ = NULL;
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
struct hash<husky::BlkDesc> {
    size_t operator()(const husky::BlkDesc& t) const { return hash<string>()(t.filename); }
};
}  // namespace std




