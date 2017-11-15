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
#include <limits>
#include <string>
#include <iostream>
#include <iterator>
#include <vector>


namespace flexps {

GBDTController::GBDTController() {
  this->ps_key_ptr = 0;
}

void GBDTController::init(std::map<std::string, std::string> & options, std::map<std::string, float> & params) {
  
  // Initialize options and params
  this->options = options;
  this->params = params;

}

GBDTTree GBDTController::build_tree(KVClientTable<float> & table, std::vector<std::vector<float>> feat_vect_list, std::vector<std::map<std::string, float>> min_max_feat_list, 
  std::vector<float> grad_vect, std::vector<float> hess_vect) {
  GBDTTree tree = create_tree();

  tree.train(this->ps_key_ptr, table, feat_vect_list, min_max_feat_list, grad_vect, hess_vect);

  return tree;
}

GBDTTree GBDTController::create_tree() {
  return GBDTTree(this->params);
}

void GBDTController::build_forest(KVClientTable<float> & table, std::vector<float> class_vect, std::vector<std::vector<float>> feat_vect_list, std::vector<std::map<std::string, float>> min_max_feat_list) {
  float init_estimator = 0.0;
  // Initialize estimator vector
  std::vector<float> estimator_vect(class_vect.size(), init_estimator);

  std::vector<float> grad_vect;
  std::vector<float> hess_vect;
  for (int i = 0; i < params["num_of_trees"]; i++) {
    // Step 1: Update residual vect
    LOG(INFO) << "Step 1: Update residual vect";
    grad_vect = get_gradient_vect(class_vect, estimator_vect, this->options["loss_function"], 1);
    hess_vect = get_gradient_vect(class_vect, estimator_vect, this->options["loss_function"], 2);

    // Step 2: Train tree
    LOG(INFO) << "Step 2: Train tree";
    GBDTTree tree = build_tree(table, feat_vect_list, min_max_feat_list, grad_vect, hess_vect);

    // Step 3: Update estimator vect
    LOG(INFO) << "Step 3: Update estimator vect";
    update_estimator_vect(tree, estimator_vect, class_vect, feat_vect_list);
    LOG(INFO) << "estimator_vect[38] = " << estimator_vect[38];

    // Step 4: Add tree to forest
    LOG(INFO) << "Step 4: Add tree to forest";
    this->forest.push_back(tree);
  }
}

void GBDTController::update_estimator_vect(GBDTTree & tree, std::vector<float> & estimator_vect, std::vector<float> & class_vect, std::vector<std::vector<float>> & feat_vect_list) {
  for (int i = 0; i < estimator_vect.size(); i++) {
    std::vector<float> data_row = DataLoader::get_feat_vect_by_row(feat_vect_list, i);
    float gamma = tree.predict(data_row);
    estimator_vect[i] += gamma;
  }
}

// Helper function
std::vector<float> GBDTController::get_gradient_vect(std::vector<float> class_vect, std::vector<float> estimator_vect, std::string loss_function, int order) {
  std::vector<float> residual_vect;
  for (int i = 0; i < class_vect.size(); i++) {
    float residual = flexps::calculate_gradient(class_vect[i], estimator_vect[i], loss_function, order);
    residual_vect.push_back(residual);
  }
  return residual_vect;
}

}
