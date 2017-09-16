#pragma once

#include <cinttypes>
#include <functional>
#include <map>
#include <vector>

#include "driver/info.hpp"
#include "driver/worker_spec.hpp"

namespace flexps {

class MLTask {
 public:
  void SetLambda(const std::function<void(const Info&)>& func) {
    func_ = func;
  }
  void RunLambda(const Info& info) const {
    func_(info);
  }
  void SetWorkerSpec(const WorkerSpec& worker_spec) {
    worker_spec_ = worker_spec;
  }
  const WorkerSpec& GetWorkerSpec() const {
    return worker_spec_;
  }
  void SetTables(const std::vector<uint32_t>& tables) {
    tables_ = tables;
  }
  const std::vector<uint32_t>& GetTables() const {
    return tables_;
  }
 private:
  std::function<void(const Info&)> func_;
  WorkerSpec worker_spec_;
  std::vector<uint32_t> tables_;
};

}  // namespace flexps
