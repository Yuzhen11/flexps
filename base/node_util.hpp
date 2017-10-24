#pragma once

#include <vector>
#include <string>

#include "base/node.hpp"

#include "glog/logging.h"

namespace flexps {

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
std::vector<Node> ParseFile(const std::string& filename);

/*
 * Check whether there are duplicated node ids in the nodes vector
 */
bool CheckValidNodeIds(const std::vector<Node>& nodes);

/*
 * Check whether there are duplicated port in the same host
 */
bool CheckUniquePort(const std::vector<Node>& nodes);

Node GetNodeById(const std::vector<Node>& nodes, int id);

/*
 * Return true if the nodes ids are {0, 1, 2, ... }.
 * The order also matters.
 */
bool CheckConsecutiveIds(const std::vector<Node>& nodes);

/*
 * Return true if id is in nodes, false otherwise
 */
bool HasNode(const std::vector<Node>& nodes, uint32_t id);

}  // namespace flexps

