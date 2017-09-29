#include "gtest/gtest.h"
#include "glog/logging.h" 
#include <thread>
#include "io/hdfs_assigner.hpp"
#include "io/lineInput.hpp"
#include "io/master.hpp"
#include "io/coordinator.hpp"
#include "io/file_splitter.hpp"
#include "boost/utility/string_ref.hpp"
#include "base/serialization.hpp"



namespace flexps{


void HDFS_Read() {
    std::string hdfs_namenode = "proj10";
    int hdfs_namenode_port = 9000;
    int master_port = 19817;
    zmq::context_t* zmq_context = new zmq::context_t(1);

    std::thread master_thread([zmq_context,master_port,hdfs_namenode_port,hdfs_namenode] {
        int master_port = 19817;
        auto& master = Master::get_instance();
        HDFSBlockAssigner hdfs_block_assigner(hdfs_namenode,hdfs_namenode_port);
        master.setup(master_port, zmq_context);
        master.serve();
    });

    std::thread worker_thread([zmq_context,master_port,hdfs_namenode_port,hdfs_namenode] {
		std::string master_host = "proj10";
     	std::string input = "hdfs:///datasets/classification/a9";
     	int num_threads = 1;
     	int second_id = 0;
        std::string worker_host = "proj10";
     	int proc_id = getpid();
        Coordinator* coordinator = new Coordinator(proc_id, worker_host, zmq_context, master_host, master_port);
        coordinator->serve();
        LOG(INFO)<<"Coordinator begin to serve";
        LineInputFormat infmt(input, num_threads, second_id, coordinator,worker_host,hdfs_namenode,hdfs_namenode_port);
        LOG(INFO)<<"Line input is well prepared";
        bool success = true;
        int count = 0;
        boost::string_ref record;
        while(true){
            success = infmt.next(record);
            count++;
            if(success == false)
                break;
        }
        BinStream finish_signal;
        finish_signal << worker_host << second_id;
        coordinator->notify_master(finish_signal, 300);

        LOG(INFO) << "the number of lines of netflix_small is " << count;
    });
    master_thread.join();
    worker_thread.join();
}

}

int main(){
    flexps::HDFS_Read();
}




