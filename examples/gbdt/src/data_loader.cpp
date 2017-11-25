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

DataLoader::DataLoader() {

}

DataLoader::DataLoader(std::string file_path, std::string delimiter) {
  std::ifstream infile(file_path.c_str());

  std::string line;
  bool init_flag = false;
  this->num_of_record = 0;
  this->num_of_feat = 0;

  while (getline(infile, line)) {
    std::vector<float> val_vect = read_line_to_vect(line);
    if (init_flag == false) {
      num_of_feat = val_vect.size() - 1;
      this->feat_vect_list.resize(num_of_feat);
      init_flag = true;
    }
    this->class_vect.push_back(val_vect[0]);

    for (int i = 0; i < num_of_feat; i++) {
      this->feat_vect_list[i].push_back(val_vect[i+1]);
    }

    this->num_of_record++;
  }
}

std::vector<float> DataLoader::read_line_to_vect(std::string line) {
  // line format: <class_val>  1:<feat_val> 2:<feat_val> ...

  std::vector<float> vect;
  std::vector<std::string> feat_element_vect = split(line, "  ");

  float class_val = atof(feat_element_vect[0].c_str());
  vect.push_back(class_val);

  feat_element_vect.erase(feat_element_vect.begin());

  for (std::string element: feat_element_vect) {
    float feat_val = atof(split(element, ":")[1].c_str());
    vect.push_back(feat_val);
  }

  return vect;
}

void DataLoader::read_hdfs_to_class_feat_vect(std::vector<std::string> & line_vect
      , std::vector<float> & class_vect, std::vector<std::vector<float>> & feat_vect_list) {
  DataLoader dataloader;
  bool init_flag = false;
  int num_of_feat = 0;

  for (std::string line: line_vect) {
    std::vector<float> val_vect = dataloader.read_line_to_vect(line);
    if (init_flag == false) {
      num_of_feat = val_vect.size() - 1;
      feat_vect_list.resize(num_of_feat);
      init_flag = true;
    }
    class_vect.push_back(val_vect[0]);

    for (int i = 0; i < num_of_feat; i++) {
      feat_vect_list[i].push_back(val_vect[i+1]);
    }
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