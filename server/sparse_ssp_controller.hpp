#pragma once

#include "base/message.hpp"
#include "server/abstract_sparse_ssp_recorder.hpp"

namespace flexps {

class SparseSSPController {
 public:
  SparseSSPController() = delete;
  SparseSSPController(uint32_t staleness, uint32_t speculation);

  std::list<Message> Clock(int progress, int sender, int updated_min_clock, int min_clock);

  void AddRecord(Message& msg);

 private:
  uint32_t staleness_;
  uint32_t speculation_;

  std::unique_ptr<AbstractSparseSSPRecorder> recorder_;

};

}  // namespace flexps

