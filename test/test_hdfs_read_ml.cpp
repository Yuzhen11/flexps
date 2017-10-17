#include <thread>
#include "base/serialization.hpp"
#include "boost/utility/string_ref.hpp"
#include "glog/logging.h"
#include "gtest/gtest.h"
#include "io/coordinator.hpp"
#include "io/file_splitter.hpp"
#include "io/hdfs_assigner.hpp"
#include "io/lineInput.hpp"

namespace flexps {

void HDFS_Read() {
  std::string hdfs_namenode = "proj10";
  std::string worker_host = "proj10";
  std::string master_host = "proj10";
  int hdfs_namenode_port = 9000;
  int master_port = 19817;
  zmq::context_t* zmq_context = new zmq::context_t(1);
  int proc_id = getpid();
  Coordinator coordinator(proc_id, worker_host, zmq_context, master_host, master_port);
  coordinator.serve();
  Coordinator* pcoordinator = &coordinator;
  std::thread master_thread([zmq_context, master_port, hdfs_namenode_port, hdfs_namenode] {
    HDFSBlockAssigner hdfs_block_assigner(hdfs_namenode, hdfs_namenode_port, zmq_context, master_port);
    hdfs_block_assigner.Serve();
  });
   std::thread worker_thread1([zmq_context, master_port, hdfs_namenode_port, hdfs_namenode, pcoordinator] {
     std::string master_host = "proj10";
     std::string input = "hdfs:///datasets/classification/a9";
     int num_threads = 2;
     int second_id = 0;
     std::string worker_host = "proj10";
     /*
     int proc_id = getpid();
     Coordinator coordinator(proc_id, worker_host, zmq_context, master_host, master_port);
     coordinator.serve();
     */
     LineInputFormat infmt(input, num_threads, second_id, pcoordinator, worker_host, hdfs_namenode, hdfs_namenode_port);
     LOG(INFO) << "Line input is well prepared";
     bool success = true;
     int count = 0;
     boost::string_ref record;
     while (true) {
       success = infmt.next(record);
       count++;
       if (success == false)
         break;
     }
     BinStream finish_signal;
     finish_signal << worker_host << second_id;
     pcoordinator->notify_master(finish_signal, 300);

     LOG(INFO) << "the number of lines of netflix_small is " << count;
   });


  std::thread worker_thread2([zmq_context, master_port, hdfs_namenode_port, hdfs_namenode, pcoordinator] {
    std::string master_host = "proj10";
    std::string input = "hdfs:///datasets/classification/a9";
    int num_threads = 2;
    int second_id = 1;
    std::string worker_host = "proj10";
    LineInputFormat infmt(input, num_threads, second_id, pcoordinator, worker_host, hdfs_namenode, hdfs_namenode_port);
    LOG(INFO) << "Line input is well prepared";
    bool success = true;
    int count = 0;
    boost::string_ref record;
    while (true) {
      success = infmt.next(record);
      count++;
      if (success == false)
        break;
    }
    BinStream finish_signal;
    finish_signal << worker_host << second_id;
    pcoordinator->notify_master(finish_signal, 300);

    LOG(INFO) << "the number of lines of a9 is " << count;
  });
  master_thread.join();
  worker_thread1.join();
  worker_thread2.join();
}
}

int main() { flexps::HDFS_Read(); }
