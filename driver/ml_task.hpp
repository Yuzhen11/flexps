#pragma once

#include <cinttypes>
#include <functional>
#include <map>
#include <vector>

#include "driver/info.hpp"

namespace flexps {

struct WorkerAlloc {
  uint32_t node_id;
  uint32_t num_workers;
};

class MLTask {
 public:
  void SetLambda(const std::function<void(const Info&)>& func) {
    func_ = func;
  }
  void RunLambda(const Info& info) const {
    func_(info);
  }
  void SetWorkerAlloc(const std::vector<WorkerAlloc>& worker_alloc) {
    worker_alloc_ = worker_alloc;
  }
  const std::vector<WorkerAlloc>& GetWorkerAlloc() const {
    return worker_alloc_;
  }
  void SetTables(const std::vector<uint32_t>& tables) {
    tables_ = tables;
  }
  const std::vector<uint32_t>& GetTables() const {
    return tables_;
  }
  bool IsSetup() const {
    return func_ && !worker_alloc_.empty() && !tables_.empty();
  }
 private:
  std::function<void(const Info&)> func_;
  std::vector<WorkerAlloc> worker_alloc_;
  std::vector<uint32_t> tables_;
};

}  // namespace flexps
