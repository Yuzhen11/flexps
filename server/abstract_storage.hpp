#pragma once

#include "base/message.hpp"

#include "glog/logging.h"

namespace flexps {

/*
 * Implement using the template method and dispatch the SubAdd/SubGet to subclasses.
 */
class AbstractStorage {
 public:
  void Add(Message& msg) {
    CHECK(msg.data.size() == 2);
    auto typed_keys = third_party::SArray<Key>(msg.data[0]);
    if(msg.meta.flag == Flag::kAddChunk)
      SubAddChunk(typed_keys, msg.data[1]);
    else
      SubAdd(typed_keys, msg.data[1]);
  }
  Message Get(Message& msg) {
    CHECK(msg.data.size() == 1);
    auto typed_keys = third_party::SArray<Key>(msg.data[0]);
    Message reply;
    reply.meta.recver = msg.meta.sender;
    reply.meta.sender = msg.meta.recver;
    switch (msg.meta.flag)
    {
    case Flag::kGet:
	reply.meta.flag = Flag::kGetReply;
	break;
    case Flag::kGetChunk:
	reply.meta.flag = Flag::kGetChunkReply;
	break;
    default:
	reply.meta.flag = msg.meta.flag;
	break;
    }
    reply.meta.model_id = msg.meta.model_id;
    reply.meta.version = msg.meta.version;
    third_party::SArray<Key> reply_keys(typed_keys);
    third_party::SArray<char> reply_vals;
    if(msg.meta.flag == Flag::kGetChunk)
      reply_vals = SubGetChunk(reply_keys);
    else
      reply_vals = SubGet(reply_keys);
    reply.AddData<Key>(reply_keys);
    reply.AddData<char>(reply_vals);
    return reply;
  }
  
  virtual void SubAdd(const third_party::SArray<Key>& typed_keys, 
      const third_party::SArray<char>& vals) = 0;
  virtual void SubAddChunk(const third_party::SArray<Key>& typed_keys, 
      const third_party::SArray<char>& vals) = 0;
  virtual third_party::SArray<char> SubGet(const third_party::SArray<Key>& typed_keys) = 0;
  virtual third_party::SArray<char> SubGetChunk(const third_party::SArray<Key>& typed_keys) = 0;

  virtual void FinishIter() = 0;
};

}  // namespace flexps
