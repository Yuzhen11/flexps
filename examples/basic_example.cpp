// #include "driver/engine.hpp"

#include "gflags/gflags.h"
#include "glog/logging.h"

#include "driver/node_parser.hpp"

DEFINE_int32(my_id, -1, "The process id of this program");
DEFINE_string(config_file, "", "The config file path");

namespace flexps {

void Run() {
  CHECK_NE(FLAGS_my_id, -1);
  CHECK(!FLAGS_config_file.empty());
  LOG(INFO) << FLAGS_my_id << " " << FLAGS_config_file;

  std::vector<Node> nodes = ParseFile(FLAGS_config_file);
  CHECK(CheckValidNodeIds(nodes));
  CHECK(CheckUniquePort(nodes));
  CHECK(CheckConsecutiveIds(nodes));
  Node my_node = GetNodeById(nodes, FLAGS_my_id);
  /*
  Engine engine(my_node, nodes);
  // 1. Start engine
  engine.CreateMailbox();
  engine.StartServerThreads();
  engine.StartWorkerHelperThreads();
  engine.StartMailbox();

  // 2. Create tables
  SimpleRangeManager range_manager_0({{2,5}, {5,7}});
  engine.CreateTable(0, range_manager_0);
  SimpleRangeManager range_manager_1({{2,5}, {5,7}});
  engine.CreateTable(1, range_manager_1);

  // 3. Construct tasks
  MLTask task;
  task.SetWorkerSpec(WorkerSpec());
  task.SetLambda([](const Info& info) {
  });

  // 4. Run tasks
  engine.Run(task);

  // 5. Stop engine
  engine.StopMailbox();
  engine.StopServerThreads();
  engine.StopWorkerHelperThreads();
  */
}

}  // namespace flexps

int main(int argc, char** argv) {
  gflags::ParseCommandLineFlags(&argc, &argv, true);
  google::InitGoogleLogging(argv[0]);
  flexps::Run();
}
