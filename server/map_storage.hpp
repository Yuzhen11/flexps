#pragma once

#include "base/message.hpp"
#include "server/abstract_storage.hpp"

#include "glog/logging.h"

#include <map>

namespace flexps {

template <typename T>
class MapStorage : public AbstractStorage {
 public:
  MapStorage() {}

  virtual void Add(Message& msg) override {
    CHECK(msg.data.size() == 2);
    CHECK(msg.data[0].size() == msg.data[1].size());
    // Todo(Ruoyu Wu): Type Check
    auto keys = third_party::SArray<Key>(msg.data[0]);
    auto vals = third_party::SArray<T>(msg.data[1]);
    for (size_t index = 0; index < keys.size(); index++) {
      FindAndCreate(keys[index]);
      storage_[keys[index]] += vals[index];
    }
  }

  virtual Message Get(Message& msg) override {
    CHECK(msg.data.size() == 1);
    // Todo(Ruoyu Wu): Type Check
    auto keys = third_party::SArray<Key>(msg.data[0]);
    Message reply;
    reply.meta.recver = msg.meta.sender;
    reply.meta.sender = msg.meta.recver;
    reply.meta.flag = msg.meta.flag;
    reply.meta.model_id = msg.meta.model_id;
    reply.meta.version = msg.meta.version;
    std::vector<T> reply_vec;
    for (auto& key : keys) {
      FindAndCreate(key);
      reply_vec.push_back(storage_[key]);
    }
    third_party::SArray<Key> reply_keys(keys);
    third_party::SArray<T> reply_vals(reply_vec);
    reply.AddData<Key>(reply_keys);
    reply.AddData<T>(reply_vals);
    return reply;
  }

  virtual void FinishIter() override {}

 private:
  virtual void FindAndCreate(Key key) override {
    if (storage_.find(key) == storage_.end()) {
      storage_[key] = T();
    }
  }

  std::map<Key, T> storage_;
};

}  // namespace flexps
