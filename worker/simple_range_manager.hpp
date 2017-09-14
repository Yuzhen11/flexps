#pragma once

#include "base/third_party/range.h"

#include "glog/logging.h"

#include <cinttypes>
#include <vector>

namespace flexps {

class SimpleRangeManager {
 public:
  SimpleRangeManager(const std::vector<third_party::Range>& ranges,
                     const std::vector<uint32_t>& server_thread_ids)
    : ranges_(ranges), server_thread_ids_(server_thread_ids) {
    CHECK_EQ(ranges_.size(), server_thread_ids_.size());
  }
  size_t GetNumServers() const { return ranges_.size(); }
  const std::vector<third_party::Range>& GetRanges() const { return ranges_; }
  const std::vector<uint32_t>& GetServerThreadIds() const { return server_thread_ids_; }

 private:
  std::vector<third_party::Range> ranges_;
  std::vector<uint32_t> server_thread_ids_;
};

}  // namespace flexps
