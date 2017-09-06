#include "server/abstract_storage.hpp"

#include "server/message.hpp"
#include "server/threadsafe_queue.hpp"

#include <map>

namespace flexps {

template <typename T>
class Storage : public AbstractStorage {
 public:
  Storage(ThreadsafeQueue<Message>* const threadsafe_queue) : threadsafe_queue_(threadsafe_queue) {}

  virtual void Add(const Message& message) override {
    size_t key;
    T val;
    message.bin >> key >> val;
    storage_[key] += val;
  }

  virtual void Get(const Message& message) override {
    size_t key;
    message.bin >> key;
    T res = storage_[key];
    Message rep;
    rep.bin << key << res;
    threadsafe_queue_.Push(rep);
  }

  virtual void FinishIter() override {}

 private:
  // Not owned.
  ThreadsafeQueue<Message>* const threadsafe_queue_;
  std::map<T> storage_;
};

}  // namespace flexps
