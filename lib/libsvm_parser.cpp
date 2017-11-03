#include "boost/utility/string_ref.hpp"

namespace flexps {

using DataObj = std::pair<std::vector<std::pair<int, float>>, float>;

DataObj libsvm_parser(boost::string_ref record) {
	DataObj this_obj;
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
	      this_obj.first.push_back(std::make_pair(idx, val));
	      i = 0;
	  } else {
	      this_obj.second = std::atof(tok);
	      i = 0;
	  }
	  // Next key/value pair
	  tok = strtok_r(NULL, " \t:", &pos);
	}
	return this_obj;
}
}  // namespace flexps

