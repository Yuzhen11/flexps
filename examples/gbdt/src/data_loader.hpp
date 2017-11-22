#pragma once

#include <string>
#include <vector>

namespace flexps {

class DataLoader {
  public:
    DataLoader();
  	DataLoader(std::string path, std::string delimiter);
    std::vector<float> read_line_to_vect(std::string line);
    static void read_hdfs_to_class_feat_vect(std::vector<std::string> & line_vect
      , std::vector<float> & class_vect, std::vector<std::vector<float>> & feat_vect_list);
    std::vector<float> get_class_vect();
    std::vector<std::vector<float>> get_feat_vect_list();
    static std::vector<float> get_feat_vect_by_row(std::vector<std::vector<float>> & feat_vect_list, int row);
    static void split_dataset_by_feat_val(
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
      );
  private:
    std::vector<float> class_vect;
    std::vector<std::vector<float>> feat_vect_list;
    int num_of_record;
    int num_of_feat;
};

}