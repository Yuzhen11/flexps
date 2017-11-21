#pragma once

#include "base/message.hpp"
#include "server/abstract_storage.hpp"

#include "glog/logging.h"

#include <map>

namespace flexps {

template <typename Val>
class MapStorage : public AbstractStorage {
 public:
  MapStorage(uint32_t chunk_size = 1):chunk_size_(chunk_size){}

  virtual void SubAdd(const third_party::SArray<Key>& typed_keys, 
      const third_party::SArray<char>& vals) override {
    auto typed_vals = third_party::SArray<Val>(vals);
    CHECK_EQ(chunk_size_ * typed_keys.size(), typed_vals.size());
    for (size_t index = 0; index < typed_keys.size(); index++) {
      if(storage_[typed_keys[index]].empty())
        storage_[typed_keys[index]].resize(chunk_size_);
      for (size_t chunk_index = 0; chunk_index < chunk_size_; chunk_index++) {
        storage_[typed_keys[index]][chunk_index] += typed_vals[index * chunk_size_ + chunk_index];
      }
    }
  }

  virtual third_party::SArray<char> SubGet(const third_party::SArray<Key>& typed_keys) override {
    third_party::SArray<Val> reply_vals(typed_keys.size() * chunk_size_);
    for (int i = 0; i < typed_keys.size(); ++ i) {
      if(storage_[typed_keys[i]].empty())                                                                            
        storage_[typed_keys[i]].resize(chunk_size_);
      for (int j = 0; j < chunk_size_; ++ j){
        reply_vals[i * chunk_size_ + j] = storage_[typed_keys[i]][j];
      }
    }
    return third_party::SArray<char>(reply_vals);
  }

  virtual void FinishIter() override {}

 private:
  std::map<Key, third_party::SArray<Val>> storage_;
  uint32_t chunk_size_;
};

}  // namespace flexps
