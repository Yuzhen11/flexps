#pragma once

#include "base/sarray_binstream.hpp"
#include "base/threadsafe_queue.hpp"
#include "comm/abstract_channel.hpp"

namespace flexps {

/*
 * The local channel used in each thread
 * Use BSP model.
 */
class LocalChannel {
 public:
  LocalChannel(uint32_t tid, AbstractChannel* const channel);
  LocalChannel(const LocalChannel&) = delete;
  LocalChannel& operator=(const LocalChannel&) = delete;
  uint32_t GetId() const { return tid_; }
  /*
   * Push an SArrayBinStream to remote thread.
   */
  void PushTo(uint32_t id, const SArrayBinStream& bin);
  /*
   * Sync and get messages
   */
  std::vector<SArrayBinStream> SyncAndGet();
  /*
   * Get the queue.
   * Called by Channel but not users
   */
  ThreadsafeQueue<Message>* GetQueue() { return &queue_; }
 private:
  const uint32_t tid_;
  AbstractChannel* const channel_;
  ThreadsafeQueue<Message> queue_;
};

}  // namespace flexps
