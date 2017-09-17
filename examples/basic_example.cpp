// #include "driver/engine.hpp"

#include "gflags/gflags.h"
#include "glog/logging.h"

#include "driver/node_parser.hpp"
#include "driver/engine.hpp"
#include "worker/kv_client_table.hpp"

DEFINE_int32(my_id, -1, "The process id of this program");
DEFINE_string(config_file, "", "The config file path");

namespace flexps {

void Run() {
  CHECK_NE(FLAGS_my_id, -1);
  CHECK(!FLAGS_config_file.empty());
  LOG(INFO) << FLAGS_my_id << " " << FLAGS_config_file;

  // 0. Parse config_file
  std::vector<Node> nodes = ParseFile(FLAGS_config_file);
  CHECK(CheckValidNodeIds(nodes));
  CHECK(CheckUniquePort(nodes));
  CHECK(CheckConsecutiveIds(nodes));
  Node my_node = GetNodeById(nodes, FLAGS_my_id);
  LOG(INFO) << my_node.DebugString();

  // 1. Start engine
  Engine engine(my_node, nodes);
  engine.StartEverything();

  // 2. Create tables
  const int kTableId = 0;
  const int kMaxKey = 1000;
  std::vector<third_party::Range> range;
  for (int i = 0; i < nodes.size() - 1; ++ i) {
    range.push_back({kMaxKey / nodes.size() * i, kMaxKey / nodes.size() * (i + 1)});
  }
  range.push_back({kMaxKey / nodes.size() * (nodes.size() - 1), kMaxKey});
  engine.CreateTable(kTableId, range);  // table 0, range [0,10)
  engine.Barrier();

  // 3. Construct tasks
  MLTask task;
  task.SetWorkerAlloc({{0, 3}});  // 3 workers on node 0
  task.SetTables({kTableId});  // Use table 0
  task.SetLambda([kTableId](const Info& info){
    LOG(INFO) << "Hi";
    LOG(INFO) << info.DebugString();
    CHECK(info.range_manager_map.find(kTableId) != info.range_manager_map.end());
    KVClientTable<float> table(info.thread_id, kTableId, info.send_queue, &info.range_manager_map.find(kTableId)->second, info.callback_runner);
    for (int i = 0; i < 5; ++ i) {
      std::vector<Key> keys{1};
      std::vector<float> vals{0.5};
      table.Add(keys, vals);
      std::vector<float> ret;
      table.Get(keys, &ret);
      CHECK_EQ(ret.size(), 1);
      LOG(INFO) << ret[0];
    }
  });

  // 4. Run tasks
  engine.Run(task);

  // 5. Stop engine
  engine.StopEverything();
}

}  // namespace flexps

int main(int argc, char** argv) {
  gflags::ParseCommandLineFlags(&argc, &argv, true);
  google::InitGoogleLogging(argv[0]);
  flexps::Run();
}
