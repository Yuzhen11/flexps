#pragma once

#include <map>
#include <string>
#include <vector>
#include "worker/kv_client_table.hpp"

#include "gbdt_tree.hpp"


namespace flexps {

class GBDTController {
  public:
  	GBDTController();
    void init(std::map<std::string, std::string> & options, std::map<std::string, float> & params);
    GBDTTree build_tree(KVClientTable<float> & table, std::vector<std::vector<float>> feat_vect_list, 
      std::vector<std::map<std::string, float>> min_max_feat_list, std::vector<float> grad_vect, std::vector<float> hess_vect);
    GBDTTree create_tree();
    void build_forest(KVClientTable<float> & table, std::vector<float> class_vect, std::vector<std::vector<float>> feat_vect_list, std::vector<std::map<std::string, float>> min_max_feat_list);
    void update_estimator_vect(GBDTTree & tree, std::vector<float> & estimator_vect, std::vector<float> & class_vect, std::vector<std::vector<float>> & feat_vect_list);
  protected:
    // Helper functions
    std::vector<float> get_gradient_vect(std::vector<float> class_vect, std::vector<float> estimator_vect, std::string loss_function, int order);
  private:
    int ps_key_ptr;
    std::map<std::string, std::string> options;
    std::map<std::string, float> params;

    std::vector<GBDTTree> forest;
};

}