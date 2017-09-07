#pragma once

#include <cinttypes>
#include <sstream>

#include "base/magic.hpp"
#include "base/serialization.hpp"

namespace flexps {

struct Control {};

enum class Flag : char { kExit, kClock, kAdd, kGet };
static const char* FlagName[] = {"kExit", "kClock", "kAdd", "kGet"};

struct Meta {
  uint32_t sender;
  uint32_t recver;
  uint32_t model_id;
  Flag flag;  // {kExit, kClock, kAdd, kGet}

  std::string DebugString() const {
    std::stringstream ss;
    ss << "Meta: { ";
    ss << "sender: " << sender;
    ss << ", recver: " << recver;
    ss << ", model_id: " << model_id;
    ss << ", flag: " << FlagName[static_cast<int>(flag)];

    ss << "}";
    return ss.str();
  }
};

struct Message {
  Meta meta;
  BinStream bin;
};

}  // namespace flexps
