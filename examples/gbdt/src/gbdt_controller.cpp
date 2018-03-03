#include "gbdt_controller.hpp"
#include "data_loader.hpp"
#include "math_tools.hpp"
#include "util.hpp"

#include "gflags/gflags.h"
#include "glog/logging.h"

#include "driver/engine.hpp"
#include "worker/kv_client_table.hpp"

#include <algorithm>
#include <cstdlib>
#include <map>
#include <math.h>
#include <limits>
#include <string>
#include <iostream>
#include <iterator>
#include <vector>


namespace flexps {

GBDTController::GBDTController() {
  this->ps_key_ptr = 0;
}

void GBDTController::init(std::map<std::string, std::string>& options, std::map<std::string, float>& params) {
  
  this->init_estimator = 0.0;

  // Initialize options and params
  this->options = options;
  this->params = params;

}

GBDTTree GBDTController::build_tree(int tree_id, std::map<std::string, std::unique_ptr<KVClientTable<float>>>& name_to_kv_table, std::vector<std::vector<float>>& feat_vect_list, std::vector<std::map<std::string, float>>& min_max_feat_list, 
  std::vector<float>& grad_vect, std::vector<float>& hess_vect) {
  GBDTTree tree = create_tree();

  float computation_time = 0.0;
  float communication_time = 0.0;
  tree.train(this->ps_key_ptr, name_to_kv_table, feat_vect_list, min_max_feat_list, grad_vect, hess_vect, computation_time, communication_time);

  return tree;
}

GBDTTree GBDTController::create_tree() {
  return GBDTTree(this->params);
}

void GBDTController::build_forest(std::map<std::string, std::unique_ptr<KVClientTable<float>>>& name_to_kv_table, std::vector<float>& class_vect, std::vector<std::vector<float>>& feat_vect_list, std::vector<std::map<std::string, float>>& min_max_feat_list) {
  
  // Initialize estimator vector
  std::vector<float> estimator_vect(class_vect.size(), this->init_estimator);

  std::vector<float> grad_vect;
  std::vector<float> hess_vect;

  auto forest_start_time = std::chrono::high_resolution_clock::now();
  for (int i = 0; i < params["num_of_trees"]; i++) {
    auto tree_start_time = std::chrono::high_resolution_clock::now();

    // Step 1: Update residual vect
    grad_vect = get_gradient_vect(class_vect, estimator_vect, this->options["loss_function"], 1);
    hess_vect = get_gradient_vect(class_vect, estimator_vect, this->options["loss_function"], 2);

    // Step 2: Train tree
    GBDTTree tree = build_tree(i, name_to_kv_table, feat_vect_list, min_max_feat_list, grad_vect, hess_vect);

    // Step 3: Update estimator vect
    update_estimator_vect(tree, estimator_vect, class_vect, feat_vect_list);
    
    // Step 4: Add tree to forest
    this->forest.push_back(tree);

    // Show train time and convergence information
    auto tree_end_time = std::chrono::high_resolution_clock::now();
    auto total_time = std::chrono::duration<double, std::milli>(tree_end_time - tree_start_time).count() / 1000;

    float SSE;
    int NUM;
    get_SSE_and_NUM(estimator_vect, class_vect, SSE, NUM);
    LOG(INFO) << "Node Id = [" << this->params["node_id"] << "], Worker Id = [" << this->params["worker_id"]
      << "]: Train set - SSE = [" << SSE << "], NUM = [" << NUM << "]";
  }
  auto forest_end_time = std::chrono::high_resolution_clock::now();

  auto total_time = std::chrono::duration<double, std::milli>(forest_end_time - forest_start_time).count() / 1000;
  
}

void GBDTController::update_estimator_vect(GBDTTree& tree, std::vector<float>& estimator_vect, std::vector<float>& class_vect, std::vector<std::vector<float>>& feat_vect_list) {
  for (int i = 0; i < estimator_vect.size(); i++) {
    std::vector<float> data_row = DataLoader::get_feat_vect_by_row(feat_vect_list, i);
    float gamma = tree.predict(data_row);
    estimator_vect[i] += gamma;
  }
}

float GBDTController::predict(std::vector<float>& data_row) {
  float estimator = this->init_estimator;

  for (GBDTTree tree: this->forest) {
    estimator += tree.predict(data_row);
  }

  return estimator;
}

void GBDTController::calculate_predict_result(std::vector<std::vector<float>>& test_feat_vect_list, std::vector<float>& test_class_vect, std::map<std::string, float>& predict_result) {
  LOG(INFO) << "Evaluating prediction result for test dataset...";

  float SSE = 0.0;

  for (int i = 0; i < test_class_vect.size(); i++) {
    std::vector<float> data_row = DataLoader::get_feat_vect_by_row(test_feat_vect_list, i);
    float predict = this->predict(data_row);

    float error = test_class_vect[i] - predict;
    SSE += error * error;
  }
  
  predict_result["sse"] = SSE;
  predict_result["num"] = test_class_vect.size();
}

// Helper function
std::vector<float> GBDTController::get_gradient_vect(std::vector<float>& class_vect, std::vector<float>& estimator_vect, std::string loss_function, int order) {
  std::vector<float> residual_vect;
  for (int i = 0; i < class_vect.size(); i++) {
    float residual = flexps::calculate_gradient(class_vect[i], estimator_vect[i], loss_function, order);
    residual_vect.push_back(residual);
  }
  return residual_vect;
}

}
