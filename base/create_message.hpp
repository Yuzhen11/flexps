#pragma once

#include "base/message.hpp"

namespace flexps {

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

} // namespace flexps