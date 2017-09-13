#include "worker/abstract_range_manager.hpp"

#include "base/third_party/range.h"

#include <vector>

namespace flexps {

class SimpleRangeManager : public AbstractRangeManager {
 public:
  SimpleRangeManager(const std::vector<third_party::Range>& ranges) : ranges_(ranges) {}
  virtual size_t GetNumServers() const override { return ranges_.size(); }
  virtual const std::vector<third_party::Range>& GetRanges() const override { return ranges_; }

 private:
  std::vector<third_party::Range> ranges_;
};

}  // namespace flexps
