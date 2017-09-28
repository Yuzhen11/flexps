#pragma once

#include "base/message.hpp"

namespace flexps {

class AbstractCounting {
 public:
  virtual void Add(const third_party::SArray<uint32_t> keys) = 0;
  virtual void Remove(const third_party::SArray<uint32_t> keys) = 0;
  virtual void RemoveAll() = 0;
  virtual uint32_t Count(const uint32_t& key) = 0;
  virtual ~AbstractCounting() {}
};

}  // namespace flexps
