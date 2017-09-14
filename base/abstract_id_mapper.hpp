#pragma once

#include <cinttypes>

namespace flexps {

class AbstractIdMapper {
 public:
  virtual ~AbstractIdMapper() {}
  virtual uint32_t GetNodeIdForThread(uint32_t tid) = 0;
};

}  // namespace flexps
