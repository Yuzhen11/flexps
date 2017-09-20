#pragma once

#include "base/message.hpp"
#include "server/abstract_storage.hpp"

#include "glog/logging.h"

#include <map>

namespace flexps {

template <typename Val>
class MapStorage : public AbstractStorage {
 public:
  MapStorage() = default;

  virtual void SubAdd(const third_party::SArray<Key>& typed_keys, 
      const third_party::SArray<char>& vals) override {
    auto typed_vals = third_party::SArray<Val>(vals);
    CHECK_EQ(typed_keys.size(), typed_vals.size());
    for (size_t index = 0; index < typed_keys.size(); index++) {
      storage_[typed_keys[index]] += typed_vals[index];
    }
  }

  virtual third_party::SArray<char> SubGet(const third_party::SArray<Key>& typed_keys) override {
    third_party::SArray<Val> reply_vals(typed_keys.size());
    for (int i = 0; i < typed_keys.size(); ++ i) {
      reply_vals[i] = storage_[typed_keys[i]];
    }
    return third_party::SArray<char>(reply_vals);
  }

  virtual void FinishIter() override {}

 private:
  std::map<Key, Val> storage_;
};

}  // namespace flexps
