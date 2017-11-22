#pragma once

#include "base/third_party/range.h"
#include "worker/abstract_partition_manager.hpp"
#include "worker/kvpairs.hpp"

#include "glog/logging.h"

#include <cinttypes>
#include <vector>

namespace flexps {

class SimpleRangePartitionManager : public AbstractPartitionManager {
 public:
  SimpleRangePartitionManager(const std::vector<third_party::Range>& ranges,
                              const std::vector<uint32_t>& server_thread_ids)
      : AbstractPartitionManager(server_thread_ids), ranges_(ranges) {
    CHECK_EQ(ranges_.size(), server_thread_ids_.size());
  }

  size_t GetNumServers() const { return ranges_.size(); }
  const std::vector<third_party::Range>& GetRanges() const { return ranges_; }
  const std::vector<uint32_t>& GetServerThreadIds() const { return server_thread_ids_; }

  // slice key-value pairs into <server_id, key_value_partition> pairs
  SlicedKVs Slice(const KVPairs<char>& send) const override {
    SlicedKVs sliced;
    int n_servers = GetNumServers();
    sliced.reserve(n_servers);

    auto begin = std::lower_bound(send.keys.begin(), send.keys.end(), ranges_[0].begin());
    size_t begin_idx = begin - send.keys.begin();
    auto ratio = send.vals.size() / send.keys.size();
    for (int i = 0; i < n_servers; ++i) {
      begin = std::lower_bound(begin, send.keys.end(), ranges_[i].end());
      auto split = begin - send.keys.begin();  // split index for next range
      if (split > begin_idx) {                 // if some keys fall into this range
        KVPairs<char> kv;
        kv.keys = send.keys.segment(begin_idx, split);
        kv.vals = send.vals.segment(begin_idx * ratio, split * ratio);
        sliced.push_back(std::make_pair(server_thread_ids_[i], std::move(kv)));
      }
      begin_idx = split;  // start from the split index next time
    }
    return sliced;
  }

 private:
  std::vector<third_party::Range> ranges_;
};

}  // namespace flexps
