#pragma once

#include <cinttypes>
#include <vector>

#include "base/magic.hpp"
#include "base/third_party/sarray.h"
#include "worker/kvpairs.hpp"

namespace flexps {

/*
 * Implments the interface of a PartitionManager which provides the model partitioning scheme
 */
class AbstractPartitionManager {
 public:
  using Keys = third_party::SArray<Key>;
  using SlicedKVs = std::vector<std::pair<uint32_t, KVPairs<char>>>;

  AbstractPartitionManager(const std::vector<uint32_t>& server_thread_ids) : server_thread_ids_(server_thread_ids) {}

  size_t GetNumServers() const { return server_thread_ids_.size(); }
  const std::vector<uint32_t>& GetServerThreadIds() const { return server_thread_ids_; }

  // slice key-value pairs into <server_id, key_value_partition> pairs
  virtual SlicedKVs Slice(const KVPairs<char>& kvs, bool is_chunk = false) const = 0;

 protected:
  std::vector<uint32_t> server_thread_ids_;
};  // class AbstractPartitionManager

}  // namespace flexps
