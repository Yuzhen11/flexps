#pragma once

#include <type_traits>

#include "base/third_party/sarray.h"
#include "base/message.hpp"

#include "glog/logging.h"

namespace flexps {

class SArrayBinStream {
 public:
  SArrayBinStream() = default;
  ~SArrayBinStream() = default;

  size_t Size() const;

  void AddBin(const char* bin, size_t sz);

  void* PopBin(size_t sz);

  Message ToMsg() const;

  void FromMsg(const Message& msg);
 private:
  third_party::SArray<char> buffer_;
  size_t front_ = 0;
};

template <typename T>
SArrayBinStream& operator<<(SArrayBinStream& bin, const T& t) {
  static_assert(std::is_trivially_copyable<T>::value, 
        "For non trivially copyable type, serialization functions are needed");
  bin.AddBin((char*)&t, sizeof(T));
  return bin;
}

template <typename T>
SArrayBinStream& operator>>(SArrayBinStream& bin, T& t) {
  static_assert(std::is_trivially_copyable<T>::value, 
        "For non trivially copyable type, serialization functions are needed");
  t = *(T*)(bin.PopBin(sizeof(T)));
  return bin;
}

}  // namespace flexps
