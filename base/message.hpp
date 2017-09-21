#pragma once

#include <cinttypes>
#include <sstream>

#include "base/magic.hpp"
#include "base/serialization.hpp"
#include "base/third_party/sarray.h"

namespace flexps {

struct Control {};

enum class Flag : char { kExit, kBarrier, kResetWorkerInModel, kClock, kAdd, kGet };
static const char* FlagName[] = {"kExit", "kBarrier", "kResetWorkerInModel", "kClock", "kAdd", "kGet"};

struct Meta {
  int sender;
  int recver;
  int model_id;
  Flag flag;  // {kExit, kBarrier, kResetWorkerInModel, kClock, kAdd, kGet}
  uint32_t version;

  std::string DebugString() const {
    std::stringstream ss;
    ss << "Meta: { ";
    ss << "sender: " << sender;
    ss << ", recver: " << recver;
    ss << ", model_id: " << model_id;
    ss << ", flag: " << FlagName[static_cast<int>(flag)];
    ss << ", version: " << version;

    ss << "}";
    return ss.str();
  }
};

struct Message {
  Meta meta;
  std::vector<third_party::SArray<char>> data;

  template <typename V>
  void AddData(const third_party::SArray<V>& val) {
    data.push_back(third_party::SArray<char>(val));
  }

  std::string DebugString() const {
    std::stringstream ss;
    ss << meta.DebugString();
    if (data.size()) {
      ss << " Body:";
      for (const auto& d : data)
        ss << " data_size=" << d.size();
    }
    return ss.str();
  }
};

namespace {
Message CreateMessage(Flag _flag, int _model_id, int _sender, int _recver, int _version, 
                third_party::SArray<int> keys = {}, third_party::SArray<int> values = {}) {
  Message m;
  m.meta.flag = _flag;
  m.meta.model_id = _model_id;
  m.meta.sender = _sender;
  m.meta.recver = _recver;
  m.meta.version = _version;

  if (keys.size() != 0)
    m.AddData(keys);
  if (values.size() != 0)
    m.AddData(values);
  return m;
}
}  // namespace

}  // namespace flexps
