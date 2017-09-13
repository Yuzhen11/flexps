#pragma once

#include <map>
#include <vector>

namespace flexps {

class WorkerSpec {
 public:
  // {{0, 3}, {1, 2}}: 3 workers on proc 0, 2 workers on proc 1.
  WorkerSpec(const std::map<uint32_t, uint32_t>& proc_workers) {
    Init(proc_workers);
  }
  bool HasLocalWorkers(uint32_t proc_id) const {
    return proc_to_workers.find(proc_id) != proc_to_workers.end();
  }
  const std::vector<uint32_t>& GetLocalWorkers(uint32_t proc_id) const {
    auto it = proc_to_workers.find(proc_id);
    CHECK(it != proc_to_workers.end());
    return *it;
  }
 private:
  Init(const std::map<uint32_t, uint32_t>& proc_workers) {
    uint32_t worker_count = 0;
    for (const auto& kv : proc_workers) {
      for (int i = 0; i < kv.second; ++ i) {
        workers_to_proc[num_workers] = kv.first;
        proc_to_workers[kv.first].push_back(num_workers);
        num_workers += 1;
      }
    }
  }

  // {0,0}, {1,0}, {2,0}, {3,1}, {4,1}
  std::map<uint32_t, uint32_t> workers_to_proc;
  // {0, {0,1,2}}, {1, {3,4}}
  std::map<uint32_t, std::vector<uint32_t>> proc_to_workers;
  uint32_t num_workers = 0;
};

}  // namespace flexps
