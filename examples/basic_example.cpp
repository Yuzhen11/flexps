

namespace flexps {

void Run() {
  Engine engine;
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
}

}  // namespace flexps

int main() {
  flexps::Run();
}
