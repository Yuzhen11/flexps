#pragma once

#include "server/abstract_storage.hpp"
#include "base/message.hpp"

#include "glog/logging.h"

#include <map>

namespace flexps {

template <typename T>
class Storage : public AbstractStorage {
 public:
  Storage() {}

  // Storage(ThreadsafeQueue<Message>* const threadsafe_queue) : threadsafe_queue_(threadsafe_queue) {}

  virtual void Add(Message& message) override {
    // TODO(Ruoyu Wu): check if message is legal
    int size;
    message.bin >> size;
    while (size --) {
      // size_t key;
      int key;
      T val;
      message.bin >> key >> val;
      FindAndCreate(key);
      storage_[key] += val;
    }
  }

  virtual Message Get(Message& message) override {
    // TODO(Ruoyu Wu): check if message is legal
    int size;
    Message rep;
    message.bin >> size;
    while (size --) {
      int key;
      message.bin >> key;
      FindAndCreate(key);
      rep.bin << key << storage_[key];
    }
    return rep;    
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
