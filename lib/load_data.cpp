#include <thread>
#include "glog/logging.h"
#include "gflags/gflags.h"
#include "boost/utility/string_ref.hpp"
#include "io/hdfs_manager.hpp"
#include "lib/labeled_point.cpp"

namespace flexps {

template <typename T>
std::vector<T> load_data(Node my_node, const std::vector<Node>& nodes, const HDFSManager::Config& config, int num_threads_per_node) {

	zmq::context_t* zmq_context = new zmq::context_t(1);
	HDFSManager hdfs_manager(my_node, nodes, config, zmq_context, num_threads_per_node);
	LOG(INFO) << "manager set up";
	hdfs_manager.Start();
	LOG(INFO) << "manager start";

	std::vector<T> data;
	std::mutex mylock;
	hdfs_manager.Run([my_node, &data, &mylock](HDFSManager::InputFormat* input_format, int local_tid) {

	while (input_format->HasRecord()) {

		auto record = input_format->GetNextRecord();

		if (record.empty()) return;
		T this_obj;
		char* pos;
		std::unique_ptr<char> record_ptr(new char[record.size() + 1]);
		strncpy(record_ptr.get(), record.data(), record.size());
		record_ptr.get()[record.size()] = '\0';
		char* tok = strtok_r(record_ptr.get(), " \t:", &pos);

		int i = -1;
		int idx;
		float val;
		while (tok != NULL) {
		  if (i == 0) {
		      idx = std::atoi(tok) - 1;
		      i = 1;
		  } else if (i == 1) {
		      val = std::atof(tok);
		      this_obj.x.push_back(FeaValPair<float>(idx, val));
		      i = 0;
		  } else {
		      this_obj.y = std::atof(tok);
		      i = 0;
		  }
		  // Next key/value pair
		  tok = strtok_r(NULL, " \t:", &pos);
		}
		mylock.lock();    
		data.push_back(this_obj);
		mylock.unlock();
	}
	LOG(INFO) << data.size() << " lines in (node, thread):(" 
	  << my_node.id << "," << local_tid << ")";
	});
	hdfs_manager.Stop();
	LOG(INFO) << "Finished loading data!";
	int count = 0;
	for (int i = 0; i < 10; i++) {
	count += data[i].x.size();
	}
	LOG(INFO) << "Estimated number of non-zero: " << count / 10;

	return data;

}

}  // namespace flexps

