#pragma once

#include <mutex>
#include <condition_variable>

#include "comm/abstract_mailbox.hpp"
#include "comm/abstract_channel.hpp"
#include "comm/local_channel.hpp"

namespace flexps {

class Channel : public AbstractChannel {
 public:
  Channel(uint32_t num_local_threads, uint32_t num_global_threads,
          std::vector<uint32_t> local_thread_ids, std::unordered_map<uint32_t, uint32_t> id_map,
          AbstractMailbox* mailbox);
  Channel(const Channel&) = delete;
  Channel& operator=(const Channel&) = delete;

  virtual ~Channel() override;

  /*
   * Get a vector of local_channels_.
   * The size of the vector is equal to num_local_threads_.
   * This function should be called in the main threads of all the processes.
   */
  std::vector<LocalChannel*> GetLocalChannels();


  virtual void PushTo(uint32_t id, const SArrayBinStream& bin) override;
  virtual void Wait() override;
 private:
  void RegisterQueues();
  void DeregisterQueues();
 private:
  const uint32_t num_local_threads_;
  const uint32_t num_global_threads_;
  std::vector<uint32_t> local_thread_ids_;
  // Map [0, num_global_threads_) to the threads allocated.
  std::unordered_map<uint32_t, uint32_t> id_map_;
  // not owned
  AbstractMailbox* mailbox_;

  std::vector<std::unique_ptr<LocalChannel>> local_channels_;

  // Sync-related variables
  std::mutex mu_;
  std::condition_variable cv_;
  uint32_t m_generation_ = 0;
  uint32_t m_count_;
  const uint32_t m_threshold_;
};

}  // namespace flexps
