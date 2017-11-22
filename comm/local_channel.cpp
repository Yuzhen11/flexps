#include "comm/local_channel.hpp"

namespace flexps {

LocalChannel::LocalChannel(uint32_t tid, AbstractChannel* const channel)
    :tid_(tid), channel_(channel) {
}

void LocalChannel::PushTo(uint32_t id, const SArrayBinStream& bin) {
  channel_->PushTo(id, bin);
}

std::vector<SArrayBinStream> LocalChannel::SyncAndGet() {
  channel_->Wait();
  std::vector<SArrayBinStream> rets;
  // We use this while loop because we assume all the incoming data are
  // already in the queue
  while (queue_.Size()) {
    Message msg;
    queue_.WaitAndPop(&msg);
    SArrayBinStream bin;
    bin.FromMsg(msg);
    rets.push_back(std::move(bin));
  }
  channel_->Wait();
  return rets;
}

}  // namespace flexps
