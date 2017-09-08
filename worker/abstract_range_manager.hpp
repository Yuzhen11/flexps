#pragma once

#include "base/third_party/range.h"

#include <vector>

namespace flexps {

class AbstractRangeManager {
 public:
  virtual ~AbstractRangeManager() {}
  virtual size_t GetNumServers() const = 0;
  virtual const std::vector<third_party::Range>& GetRanges() const = 0;
};

}  // namespace flexps
