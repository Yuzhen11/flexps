#include "gtest/gtest.h"
#include "glog/glog.h" 
#include <thread>

#include "lineInput.hpp"
#include "master.hpp"
#include "coordinator.hpp"
#include "file_splitter.hpp"
#include "boost/utility/string_ref.hpp"


namespace flexps {

class TestLineInput : public testing::Test {
   public:
    TestLineInput() {}
    ~TestLineInput() {}

   protected:
    void SetUp() {
    }
    void TearDown() {
    }

};

TEST_F(TestLineInput, Read) {
    std::string master_host = "proj10";
    int master_port = 01817; 
    std::string input = "hdfs:///dataset/ml/netflix_small.txt";
    std::string hdfs_namenode = "proj10"
    int hdfs_namenode_port = 9000;
    std::string worker_host = "proj10";
    int worker_port = 81817;
    int num_threads = 2;
    int first_id = 0;
    int second_id = 1;
    int proc_id = getpid();
    zmq::context_t zmq_context;
    

    std::thread master_thread([] {
        auto& master = Master::get_instance();
        HDFSBlockAssigner hdfs_block_assigner;
        
        master.setup(master_port);
        master.serve();
    });

    std::thread worker_thread([] {
        Coordinator* coordinator = new Coordinator(proc_id, worker_host, zmq_context, master_host, master_port);
        coordinator->serve();
        HDFSFileSplitterML* splitter = new HDFSFileSplitterML(num_threads,second_id,coordinator,worker_host,hdfs_namenode,hdfs_namenode_port);
        LineInputFormatML infmt(input, num_threads, second_id, splitter);
        infmt.set_input(input);
        bool success = false;
        int count = 0;
        boost::stirng_ref record;
        while(true){
            infmt.next(record);
            count++;
        }
        LOG << "the number of lines of netflix_small is " << count;
    });
}




}  // namespace ml