#include "io/file_splitter.hpp"

#include <string>

namespace flexps {

HDFSFileSplitter::HDFSFileSplitter(int num_threads, int id, Coordinator* coordinator, std::string hostname,
                                   std::string hdfs_namenode, int hdfs_namenode_port) {
  data_ = nullptr;
  num_threads_ = num_threads;
  id_ = id;
  coordinator_ = coordinator;
  hostname_ = hostname;
  hdfs_namenode_ = hdfs_namenode;
  hdfs_namenode_port_ = hdfs_namenode_port;
}

HDFSFileSplitter::~HDFSFileSplitter() {
  if (data_)
    delete[] data_;
}

boost::string_ref HDFSFileSplitter::fetch_block(bool is_next = false) {
  int nbytes = 0;

  if (is_next) {
    nbytes = hdfsRead(fs_, file_, data_, hdfs_block_size);
    if (nbytes == 0)
      return "";
    if (nbytes == -1) {
      LOG(ERROR) << "read next block error!";
    }
  } else {
    // ask master for a new block
    BinStream question;
    question << url_ << hostname_ << num_threads_ << id_;
    // 301 is constant for kIOHDFSSubsetLoad
    BinStream answer = coordinator_->ask_master(question, 301);
    std::string fn;
    answer >> fn;
    answer >> offset_;
    if (fn.empty()) {
      // no more files
      return "";
    }

    if (file_ != NULL) {
      int rc = hdfsCloseFile(fs_, file_);
      CHECK(rc == 0) << "close file fails";
      // Notice that "file" will be deleted inside hdfsCloseFile
      file_ = NULL;
    }

    // read block
    nbytes = read_block(fn);
  }
  return boost::string_ref(data_, nbytes);
}

void HDFSFileSplitter::init_blocksize(hdfsFS fs, const std::string& url) {
  int num_files;
  hdfsFileInfo* file_info = hdfsListDirectory(fs, url.c_str(), &num_files);
  for (int i = 0; i < num_files; ++i) {
    if (file_info[i].mKind == kObjectKindFile) {
      hdfs_block_size = file_info[i].mBlockSize;
      hdfsFreeFileInfo(file_info, num_files);
      return;
    }
    continue;
  }
  LOG(ERROR) << "Block size init error. (File NOT exist or EMPTY directory)";
}
void HDFSFileSplitter::load(const std::string& url) {
  // init url, fs_, hdfs_block_size
  url_ = url;
  // init fs_
  struct hdfsBuilder* builder = hdfsNewBuilder();
  hdfsBuilderSetNameNode(builder, hdfs_namenode_.c_str());
  hdfsBuilderSetNameNodePort(builder, hdfs_namenode_port_);
  fs_ = hdfsBuilderConnect(builder);
  hdfsFreeBuilder(builder);
  init_blocksize(fs_, url_);
  data_ = new char[hdfs_block_size];
}

int HDFSFileSplitter::read_block(const std::string& fn) {
  file_ = hdfsOpenFile(fs_, fn.c_str(), O_RDONLY, 0, 0, 0);
  CHECK(file_ != NULL) << "Hadoop file ile open fails";
  hdfsSeek(fs_, file_, offset_);
  size_t start = 0;
  size_t nbytes = 0;
  while (start < hdfs_block_size) {
    // only 128KB per hdfsRead
    nbytes = hdfsRead(fs_, file_, data_ + start, hdfs_block_size);
    start += nbytes;
    if (nbytes == 0)
      break;
  }
  return start;
}

}  // namespace flexps
