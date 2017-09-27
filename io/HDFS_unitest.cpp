#include "gtest/gtest.h"
#include "glog/glog.h" 
#include <thread>

#include "lineInput.hpp"
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


    auto& master = Master::get_instance();
    master.setup();
    master.serve();
    std::string master_host = "proj10";
    int master_port = 01817; 
    std::string input = "hdfs:///dataset/ml/netflix_small.txt";
    LineInputFormat infmt;
    infmt.set_input("hdfs://ml/netflix_small");
    bool success = false;
    int count = 0;
    boost::stirng_ref record;
    while(true){
        infmt.next(record);
        count++;
    }
    LOG << "the number of lines of netflix_small is " << count;
}




}  // namespace ml