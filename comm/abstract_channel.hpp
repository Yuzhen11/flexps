#pragma once

#include "base/sarray_binstream.hpp"

namespace flexps {

class AbstractChannel {
 public:
  virtual ~AbstractChannel() = default;
  virtual void PushTo(uint32_t id, const SArrayBinStream& bin) = 0;
  virtual void Wait() = 0;
};

}  // namespace flexps
