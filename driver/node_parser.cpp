#include "driver/node_parser.hpp"

#include "glog/logging.h"

#include <fstream>
#include <stdexcept>
#include <set>
#include <map>

namespace flexps {

std::vector<Node> ParseFile(const std::string& filename) {
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
      nodes.push_back(std::move(node));
    }
    catch(const std::invalid_argument& ia) {
      LOG(FATAL) << "Invalid argument: " << ia.what() << "\n";
    }
  }
  return nodes;
}

bool CheckValidNodeIds(const std::vector<Node>& nodes) {
  std::set<uint32_t> ids;
  for (const auto& node : nodes) {
    if (ids.find(node.id) != ids.end()) {
      return false;
    }
    ids.insert(node.id);
  }
  return true;
}

bool CheckUniquePort(const std::vector<Node>& nodes) {
  std::map<std::string, std::set<uint32_t>> host_ports;
  for (const auto& node : nodes) {
    auto& ports = host_ports[node.hostname];
    if (ports.find(node.port) != ports.end()) {
      return false;
    }
    ports.insert(node.port);
  }
  return true;
}

Node GetNodeById(const std::vector<Node>& nodes, int id) {
  for (const auto& node : nodes) {
    if (id == node.id) {
      return node;
    }
  }
  CHECK(false) << "Node" << id << " is not in the given node list";
}

bool CheckConsecutiveIds(const std::vector<Node>& nodes) {
  for (int i = 0; i < nodes.size(); ++ i) {
    if (nodes[i].id != i) {
      return false;
    }
  }
  return true;
}

}  // namespace flexps
