#include "examples/gbdt/src/data_loader.hpp"
#include "examples/gbdt/src/util.hpp"

#include "gflags/gflags.h"
#include "glog/logging.h"

#include <iostream>
#include <string>
#include <sstream>
#include <fstream>
#include <cstdlib>
#include <vector>

namespace flexps {

DataLoader::DataLoader(std::string file_path, std::string delimiter) {
  std::ifstream infile(file_path.c_str());

  std::string line;
  bool init_flag = false;
  this->num_of_record = 0;
  this->num_of_feat = 0;
  while (getline(infile, line)) {
    std::vector<std::string> tokens = split(line, "\t");
    if (init_flag == false) {
      num_of_feat = tokens.size() - 1;
      this->feat_vect_list.resize(num_of_feat);
      init_flag = true;
    }
    this->class_vect.push_back(atof(tokens[0].c_str()));

    for (int i = 0; i < num_of_feat; i++) {
      this->feat_vect_list[i].push_back(atof(tokens[i+1].c_str()));
    }
    this->num_of_record++;
  }
}

std::vector<float> DataLoader::get_class_vect() {
	return this->class_vect;
}

std::vector<std::vector<float>> DataLoader::get_feat_vect_list() {
	return this->feat_vect_list;
}

std::vector<float> DataLoader::get_feat_vect_by_row(std::vector<std::vector<float>> & feat_vect_list, int row) {
  std::vector<float> data_row;
  for (int j = 0; j < feat_vect_list.size(); j++) {
    data_row.push_back(feat_vect_list[j][row]);
  }
  return data_row;
}

void DataLoader::split_dataset_by_feat_val(
      int feat_id,
      float split_val,
      std::vector<float> & orig_grad_vect,
      std::vector<float> & left_grad_vect,
      std::vector<float> & right_grad_vect,
      std::vector<float> & orig_hess_vect,
      std::vector<float> & left_hess_vect,
      std::vector<float> & right_hess_vect,
      std::vector<std::vector<float>> & orig_feat_vect_list,
      std::vector<std::vector<float>> & left_feat_vect_list,
      std::vector<std::vector<float>> & right_feat_vect_list
      ) {
  left_feat_vect_list.resize(orig_feat_vect_list.size());
  right_feat_vect_list.resize(orig_feat_vect_list.size());

  for (int i = 0; i < orig_grad_vect.size(); i++) {
    if (orig_feat_vect_list[feat_id][i] < split_val) {
      left_grad_vect.push_back(orig_grad_vect[i]);
      left_hess_vect.push_back(orig_hess_vect[i]);
      for (int j = 0; j < orig_feat_vect_list.size(); j++) {
        left_feat_vect_list[j].push_back(orig_feat_vect_list[j][i]);
      }
    }
    else {
      right_grad_vect.push_back(orig_grad_vect[i]);
      right_hess_vect.push_back(orig_hess_vect[i]);
      for (int j = 0; j < orig_feat_vect_list.size(); j++) {
        right_feat_vect_list[j].push_back(orig_feat_vect_list[j][i]);
      }
    }
  }
}

}