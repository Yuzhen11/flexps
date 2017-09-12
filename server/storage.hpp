#pragma once

#include "base/message.hpp"
#include "server/abstract_storage.hpp"

#include "glog/logging.h"

#include <map>

namespace flexps {

template <typename T>
class Storage : public AbstractStorage {
 public:
  Storage() {}

  // Storage(ThreadsafeQueue<Message>* const threadsafe_queue) : threadsafe_queue_(threadsafe_queue) {}

  virtual void Add(Message& message) override {
    CHECK(message.data.size() == 2);
    CHECK(message.data[0].size() == message.data[1].size());
    // Todo(Ruoyu Wu): Type Check
    auto keys = third_party::SArray<int>(message.data[0]);
    auto vals = third_party::SArray<T>(message.data[1]);
    for (int index = 0; index < keys.size(); index++) {
      FindAndCreate(keys[index]);
      storage_[keys[index]] += vals[index];
    }
  }

  virtual Message Get(Message& message) override {
    CHECK(message.data.size() == 1);
    // Todo(Ruoyu Wu): Type Check
    auto keys = third_party::SArray<T>(message.data[0]);
    Message reply;
    std::vector<T> reply_vec;
    for (auto& key : keys) {
      FindAndCreate(key);
      reply_vec.push_back(storage_[key]);
    }
    third_party::SArray<int> reply_keys(keys);
    third_party::SArray<int> reply_vals(reply_vec);
    reply.AddData<int>(reply_keys);
    reply.AddData<T>(reply_vals);
    return reply;
  }

  virtual void FinishIter() override {}

 private:
  virtual void FindAndCreate(int key) override {
    if (storage_.find(key) == storage_.end()) {
      storage_[key] = T();
    }
  }

  std::map<int, T> storage_;
};

}  // namespace flexps
