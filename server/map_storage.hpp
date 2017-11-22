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
    if (chunk_size_ == 1){
      for (size_t i = 0; i < typed_keys.size(); i++) {
        storage_[typed_keys[i]] += typed_vals[i];
      }
    }
    else{
      for (size_t i = 0; i < typed_keys.size(); i++) {
        for (size_t j = 0; j < chunk_size_; j++) {
          storage_[typed_keys[i] * chunk_size_ + j] += typed_vals[i * chunk_size_ + j];
        }
      }
    }
  }

  virtual third_party::SArray<char> SubGet(const third_party::SArray<Key>& typed_keys) override {
    third_party::SArray<Val> reply_vals(typed_keys.size() * chunk_size_);
    if (chunk_size_ == 1){
      for (size_t i = 0; i < typed_keys.size(); i++) {
        reply_vals[i] = storage_[typed_keys[i]];
      }
    }
    else{
      for (int i = 0; i < typed_keys.size(); i++) {
        for (int j = 0; j < chunk_size_; j++){
          reply_vals[i * chunk_size_ + j] = storage_[typed_keys[i] * chunk_size_ + j];
        }
      }  
    }
    return third_party::SArray<char>(reply_vals);
  }

  virtual void FinishIter() override {}

 private:
  std::map<Key, Val> storage_;
  uint32_t chunk_size_;
};

}  // namespace flexps
