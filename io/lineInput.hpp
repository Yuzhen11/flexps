#pragma once

#include <string>
#include "boost/utility/string_ref.hpp"
#include "coordinator.hpp"
#include "file_splitter.hpp"
#include "glog/logging.h"

namespace flexps {

class LineInputFormat {
 public:
  LineInputFormat(const std::string url, int num_threads, int id, Coordinator* coordinator, std::string hostname,
                  std::string hdfs_namenode, int hdfs_namenode_port) {
    num_threads_ = num_threads;
    id_ = id;
    splitter_ = new HDFSFileSplitter(num_threads, id, coordinator, hostname, hdfs_namenode, hdfs_namenode_port);
    // set_up url
    set_splitter(url);
  }

  virtual ~LineInputFormat() {}

  /// function for creating different splitters for different urls
  void set_splitter(const std::string& url) {
    if (!url_.empty() && url_ == url)
      // Setting with a same url last time will do nothing.
      return;
    url_ = url;

    int prefix = url_.find("://");
    CHECK(prefix != std::string::npos) << ("Cannot analyze protocol from " + url_).c_str();
    // parse the url for hadoop
    splitter_->load(url_.substr(prefix + 3));
  }

  void set_num_threads(int num_threads) { num_threads_ = num_threads; }

  void set_worker_info(int id) { id_ = id; }

  bool next(boost::string_ref& ref) {
    if (buffer_.size() == 0) {
      bool success = fetch_new_block();
      if (success == false)
        return false;
    }
    // last charater in block
    if (r == buffer_.size() - 1) {
      // fetch next block
      buffer_ = splitter_->fetch_block(true);
      if (buffer_.empty()) {
        // end of a file
        bool success = fetch_new_block();
        if (success == false)
          return false;
      } else {
        // directly process the remaing
        last_part_ = "";
        handle_next_block();
        ref = last_part_;
        return true;
      }
    }
    if (splitter_->get_offset() == 0 && r == 0) {
      // begin of a file
      l = 0;
      if (buffer_[0] == '\n')
        // for the case file starting with '\n'
        l = 1;
      r = find_next(buffer_, l, '\n');
    } else {
      // what if not found
      l = find_next(buffer_, r, '\n') + 1;
      r = find_next(buffer_, l, '\n');
    }
    // if the right end does not exist in current block
    if (r == boost::string_ref::npos) {
      auto last = buffer_.substr(l);
      last_part_ = std::string(last.data(), last.size());
      // fetch next subBlock
      buffer_ = splitter_->fetch_block(true);
      handle_next_block();
      ref = last_part_;
      return true;
    } else {
      ref = buffer_.substr(l, r - l);
      return true;
    }
  }

 private:
  // helper function to locate the buffer
  size_t find_next(boost::string_ref sref, size_t l, char c) {
    size_t r = l;
    while (r != sref.size() && sref[r] != c)
      r++;
    if (r == sref.size())
      return boost::string_ref::npos;
    return r;
  }

  void handle_next_block() {
    while (true) {
      if (buffer_.empty())
        return;

      r = find_next(buffer_, 0, '\n');
      if (r == boost::string_ref::npos) {
        // fetch next subBlock
        last_part_ += std::string(buffer_.data(), buffer_.size());
        buffer_ = splitter_->fetch_block(true);
        continue;
      } else {
        last_part_ += std::string(buffer_.substr(0, r).data(), r);
        clear_buffer();
        return;
      }
    }
  }

  bool fetch_new_block() {
    // fetch a new block
    buffer_ = splitter_->fetch_block(false);
    if (buffer_.empty())
      //  no more files, exit
      return false;
    l = r = 0;
    return true;
  }

  void clear_buffer() {
    buffer_.clear();
    l = r = 0;
  }

  HDFSFileSplitter* splitter_;
  int num_threads_;
  int id_;
  int l = 0;
  int r = 0;
  std::string url_;
  std::string last_part_;
  boost::string_ref buffer_;
};
}
