#pragma once

#include <vector>
#include <string>
#include <fstream>
#include <stdexcept>

#include "base/node.hpp"

#include "glog/logging.h"

namespace flexps {
namespace {

/*
 * Parse a config file which should be in the format of:
 * id:hostname:port
 * ...
 *
 * Example:
 * 0:worker1:33421
 * 1:worker1:32534
 *
 * Only do the parsing, the function will failure if the input format is not correct.
 * This function does not make sure that:
 * 1. The node id is unique
 * 2. The hostname and port are valid
 */
std::vector<Node> parse_file(const std::string& filename) {
  std::vector<Node> nodes;
  std::ifstream input_file(filename.c_str());
  CHECK(input_file.is_open()) << "Error opening file: " << filename;
  std::string line;
  while (getline(input_file, line)) {
    size_t id_pos = line.find(":");
    CHECK_NE(id_pos, std::string::npos);
    std::string id = line.substr(0, id_pos);
    size_t host_pos = line.find(":", id_pos+1);
    CHECK_NE(host_pos, std::string::npos);
    std::string hostname = line.substr(id_pos+1, host_pos - id_pos - 1);
    std::string port = line.substr(host_pos+1, line.size() - host_pos - 1);
    try {
      Node node;
      node.id = std::stoi(id);
      node.hostname = std::move(hostname);
      node.port = std::stoi(port);
      LOG(INFO) << node.id << " " << node.hostname << " " << node.port;
      nodes.push_back(std::move(node));
    }
    catch(const std::invalid_argument& ia) {
      LOG(FATAL) << "Invalid argument: " << ia.what() << "\n";
    }
  }
  return nodes;
}

}  // namespace
}  // namespace flexps
