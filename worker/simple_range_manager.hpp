#pragma once

#include "base/third_party/range.h"
#include "worker/abstract_partition_manager.hpp"
#include "worker/kvpairs.hpp"

#include "glog/logging.h"

#include <cinttypes>
#include <vector>
#include <algorithm>

namespace flexps {

class SimpleRangePartitionManager : public AbstractPartitionManager {
 public:
  SimpleRangePartitionManager(const std::vector<third_party::Range>& ranges,
                              const std::vector<uint32_t>& server_thread_ids, uint32_t chunk_size = 1)
      : AbstractPartitionManager(server_thread_ids), ranges_(ranges), chunk_size_(chunk_size) {
    CHECK_EQ(ranges_.size(), server_thread_ids_.size());
  }

  size_t GetNumServers() const { return ranges_.size(); }
  const std::vector<third_party::Range>& GetRanges() const { return ranges_; }
  const std::vector<uint32_t>& GetServerThreadIds() const { return server_thread_ids_; }

  // slice key-value pairs into <server_id, key_value_partition> pairs
  SlicedKVs Slice(const KVPairs<char>& send, bool is_chunk = false) const override {
    SlicedKVs sliced;
    int n_servers = GetNumServers();
    sliced.reserve(n_servers);
    std::vector<Key> real_keys(send.keys.size());
    auto new_chunk_size_  = is_chunk ? chunk_size_ : 1;
    std::transform(send.keys.begin(), send.keys.end(), real_keys.begin(), std::bind1st(std::multiplies<Key>(),new_chunk_size_));
    auto ratio = send.vals.size()/send.keys.size();
    auto begin = std::lower_bound(real_keys.begin(), real_keys.end(), ranges_[0].begin());
    size_t begin_idx = begin - real_keys.begin();
    for (int i = 0; i < n_servers; ++i) {
      begin = std::lower_bound(begin, real_keys.end(), ranges_[i].end());
      auto split = begin - real_keys.begin();  // split index for next range
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
  uint32_t chunk_size_;
};

}  // namespace flexps
