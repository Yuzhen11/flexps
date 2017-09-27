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
  VectorStorage(third_party::Range range) : range_(range), storage_(range.size(), Val()) {
    CHECK_LE(range_.begin(), range_.end());
  }

  virtual void SubAdd(const third_party::SArray<Key>& typed_keys, 
      const third_party::SArray<char>& vals) override {
    auto typed_vals = third_party::SArray<Val>(vals);
    CHECK_EQ(typed_keys.size(), typed_vals.size());
    for (size_t index = 0; index < typed_keys.size(); index++) {
      CHECK_GE(typed_keys[index], range_.begin());
      CHECK_LT(typed_keys[index], range_.end());
      storage_[typed_keys[index] - range_.begin()] += typed_vals[index];
    }
  }

  virtual third_party::SArray<char> SubGet(const third_party::SArray<Key>& typed_keys) override {
    third_party::SArray<Val> reply_vals(typed_keys.size());
    for (int i = 0; i < typed_keys.size(); ++ i) {
      CHECK_GE(typed_keys[i], range_.begin());
      CHECK_LT(typed_keys[i], range_.end());
      reply_vals[i] = storage_[typed_keys[i] - range_.begin()];
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
};

}  // namespace flexps
