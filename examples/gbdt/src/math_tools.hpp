#pragma once

#include <map>
#include <string>
#include <vector>


namespace flexps {
  float calculate_gradient(float actual, float predict, std::string loss_function, int order);
  std::map<std::string, float> find_min_max(std::vector<float> vect);
  float calculate_rmse(std::vector<float> vect1, std::vector<float> vect2);
}