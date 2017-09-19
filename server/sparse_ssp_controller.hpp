#pragma once

#include "server/abstract_sparse_ssp_controller.hpp"
#include "server/sparse_conflict_detector.hpp"
#include "server/sparse_pending_buffer.hpp"

#include <memory>

namespace flexps {

class SparseSSPController : public AbstractSparseSSPController {
 public:
  SparseSSPController(uint32_t staleness, uint32_t speculation, 
                      std::unique_ptr<AbstractPendingBuffer>&& get_buffer,
                      std::unique_ptr<AbstractConflictDetector>&& detector)
    : staleness_(staleness), speculation_(speculation), 
    get_buffer_(std::move(get_buffer)), detector_(std::move(detector)) {}

  virtual std::vector<Message> UnblockRequests(int progress, int sender, int updated_min_clock, int min_clock) override;
  virtual void AddRecord(Message& msg) override;
 private:
  std::unique_ptr<AbstractPendingBuffer> get_buffer_;
  std::unique_ptr<AbstractConflictDetector> detector_;
  uint32_t staleness_;
  uint32_t speculation_;
};



}  // namespace flexps

