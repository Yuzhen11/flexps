#pragma once

#include <algorithm>
#include <string>
#include <map>
#include <utility>
#include <vector>

std::vector<std::string> inline split(const std::string &source, const char *delimiter = " ", bool keepEmpty = false) {
	std::vector<std::string> results;
	
	size_t prev = 0;
	size_t next = 0;
	
	while ((next = source.find_first_of(delimiter, prev)) != std::string::npos) {
		if (keepEmpty || (next - prev != 0)) {
			results.push_back(source.substr(prev, next - prev));
		}
		prev = next + 1;
	}
	if (prev < source.size()) {
		results.push_back(source.substr(prev));
	}
	return results;
}

std::vector<int> inline range(int start, int stop = -1, int step = 1) {
	std::vector<int> result;
	if (stop == -1) {
		stop = start;
		start = 0;
	}
	for (int i = start; i < stop; i += step) {
		result.push_back(i);
	}
	return result;
}

template<typename T>
void zip(std::vector<T> A, std::vector<T> B, std::vector<std::pair<T,T>> &zipped){

    for(unsigned int i=0; i< A.size(); i++){
        zipped.push_back(std::make_pair(A[i], B[i]));
    }
}

template<typename T>
int inline negative_index_convert(std::vector<T> list, int index) {
	if(index >= 0) {
		return index;
	}
	else {
		return index + list.size();
	}
}

template<typename T>
std::vector<T> inline slice(std::vector<T> list, int start = 0, int end = -1, int step = 1) {
	std::vector<T> result;
	start = negative_index_convert(list, start);
	end = negative_index_convert(list, end);
	for (int i = start; i < end; i += step) {
		T element = list[i];
		result.push_back(element);
	}
	return result;
}

template<typename T>
bool pair_compare(const std::pair<T, T>& first_ele, const std::pair<T, T>& second_ele) {
      return first_ele.first < second_ele.first;
}

template<typename T>
std::vector<std::vector<T>> inline vect_to_matrix(std::vector<T> vect, int row, int col) {
  std::vector<std::vector<T>> mat;

  for (int i = 0; i < row; i++) {
    std::vector<T> row_vect;
    for (int j = 0; j < col; j++) {
      row_vect.push_back(vect[(i * col) + j]);
    }
    mat.push_back(row_vect);
  }
  return mat;
}

// https://stackoverflow.com/questions/9370945/c-help-finding-the-max-value-in-a-map
template<typename KeyType, typename ValueType>
std::pair<KeyType, ValueType> get_max_in_map(const std::map<KeyType, ValueType> & x ) {
	using pairtype = std::pair<KeyType, ValueType>;
	return *std::max_element(x.begin(), x.end(), [] (const pairtype & p1, const pairtype & p2) {
		return p1.second < p2.second;
	});
}