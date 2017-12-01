// #include "driver/engine.hpp"

#include "gflags/gflags.h"
#include "glog/logging.h"

#include "comm/channel.hpp"

#include "base/node_util.hpp"
#include "comm/mailbox.hpp"
#include "driver/simple_id_mapper.hpp"

DEFINE_int32(my_id, -1, "The process id of this program");
DEFINE_string(config_file, "", "The config file path");

namespace flexps {

void Run() {
  CHECK_NE(FLAGS_my_id, -1);
  CHECK(!FLAGS_config_file.empty());
  VLOG(1) << FLAGS_my_id << " " << FLAGS_config_file;

  // 0. Parse config_file
  std::vector<Node> nodes = ParseFile(FLAGS_config_file);
  CHECK(CheckValidNodeIds(nodes));
  CHECK(CheckUniquePort(nodes));
  CHECK(CheckConsecutiveIds(nodes));
  Node my_node = GetNodeById(nodes, FLAGS_my_id);
  LOG(INFO) << my_node.DebugString();

  // 1. Manually start id_mapper and mailbox
  SimpleIdMapper id_mapper(my_node, nodes);
  id_mapper.Init();
  Mailbox mailbox(my_node, nodes, &id_mapper);
  mailbox.Start();

  // 2. Allocate the channel threads and create channel
  const uint32_t num_local_threads = 2;
  const uint32_t num_global_threads = num_local_threads * nodes.size();
  auto ret = id_mapper.GetChannelThreads(num_local_threads, num_global_threads);
  Channel ch(num_local_threads, num_global_threads, ret.first, ret.second, &mailbox);
  // Retrive the local_channels from channel
  auto local_channels = ch.GetLocalChannels();

  // 3. Sprawn num_local_threads threads to use local_channels
  std::vector<std::thread> threads(local_channels.size());
  for (int i = 0; i < threads.size(); ++ i) {
    threads[i] = std::thread([i, local_channels, num_global_threads]() { 
      LocalChannel* lc = local_channels[i];
      // Use the PushTo() and SyncAndGet() function to send message to other local_channels
      for (int i = 0; i < 10; ++ i) {
        LOG(INFO) << "Iter: " << i;
        // Send my id to next local_channel
        SArrayBinStream bin;
        bin << lc->GetId();
        uint32_t to = (lc->GetId()+1)%num_global_threads;
        lc->PushTo(to, bin);
        auto bins = lc->SyncAndGet();
        CHECK_EQ(bins.size(), 1);
        int res;
        bins[0] >> res;
        uint32_t from = (lc->GetId()+num_global_threads-1)%num_global_threads;
        CHECK_EQ(res, from);
      }
    });
  }
  for (auto& th: threads) {
    th.join();  
  }
  id_mapper.ReleaseChannelThreads();
  mailbox.Stop();
  LOG(INFO) << "Finish in Node " << my_node.id;
}

}  // namespace flexps

int main(int argc, char** argv) {
  gflags::ParseCommandLineFlags(&argc, &argv, true);
  google::InitGoogleLogging(argv[0]);
  flexps::Run();
}
