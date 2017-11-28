#pragma once

#include "base/message.hpp"
#include "base/third_party/range.h"
#include "server/abstract_storage.hpp"

#include "glog/logging.h"

#include <vector>

namespace flexps {

template <typename Val>
class VectorStorage : public AbstractStorage {
 public:
  VectorStorage() = delete;
  /*
   * The storage is in charge of range [range.begin(), range.end()).
   */
  VectorStorage(third_party::Range range, uint32_t chunk_size = 1) : range_(range), storage_(range.size(), Val()), chunk_size_(chunk_size) {
    CHECK_LE(range_.begin(), range_.end());
  }

  virtual void SubAdd(const third_party::SArray<Key>& typed_keys, 
      const third_party::SArray<char>& vals) override {
    auto typed_vals = third_party::SArray<Val>(vals);
   if(typed_keys.size() == typed_vals.size()) {
      for (size_t index = 0; index < typed_keys.size(); index++) { 
        CHECK_GE(typed_keys[index], range_.begin());                                                                                                                                                    
        CHECK_LT(typed_keys[index], range_.end());  
        storage_[typed_keys[index] - range_.begin()] += typed_vals[index];  
      }
    }
    else {
      CHECK_EQ(typed_vals.size()/typed_keys.size(), chunk_size_);
      for (size_t index = 0; index < typed_keys.size(); index++) {
        CHECK_GE(typed_keys[index], range_.begin());
        CHECK_LT(typed_keys[index], range_.end());
        for (size_t chunk_index = 0; chunk_index < chunk_size_; chunk_index++)
          storage_[(typed_keys[index] - range_.begin()) * chunk_size_ + chunk_index] += typed_vals[index * chunk_size_ + chunk_index];
      }
    }
  }

  virtual third_party::SArray<char> SubGet(const third_party::SArray<Key>& typed_keys, bool is_chunk = false) override {
    third_party::SArray<Val> reply_vals(typed_keys.size() * chunk_size_);
    if(!is_chunk) {
      for (size_t i = 0; i < typed_keys.size(); i++) {                                                                                                                                      
        CHECK_GE(typed_keys[i], range_.begin());                                                                                                                                                    
        CHECK_LT(typed_keys[i], range_.end());
        reply_vals[i] = storage_[typed_keys[i] - range_.begin()];
      }  
    }  
    else {
      for (int i = 0; i < typed_keys.size(); ++ i) {
        CHECK_GE(typed_keys[i], range_.begin());
        CHECK_LT(typed_keys[i], range_.end());
        for (int j = 0; j < chunk_size_; ++ j)
          reply_vals[i * chunk_size_ + j] = storage_[(typed_keys[i] - range_.begin()) * chunk_size_ + j];
      }
    }
    return third_party::SArray<char>(reply_vals);
  }

  virtual void FinishIter() override {}

  int GetBegin() {
    return range_.begin();
  }

  int GetEnd() {
    return range_.end();
  }

  size_t Size() const {
    CHECK_EQ(range_.size(), storage_.size());
    return storage_.size();
  }
 private:
  third_party::Range range_;
  std::vector<Val> storage_;
  uint32_t chunk_size_;
};

}  // namespace flexps
