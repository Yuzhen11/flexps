#pragma once

#include "base/magic.hpp"
#include "base/third_party/sarray.h"

namespace flexps {

template <typename Val>
struct KVPairs {
  third_party::SArray<Key> keys;
  third_party::SArray<Val> vals;
};

}  // namespace flexps

