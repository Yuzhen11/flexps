#pragma once

#include "base/message.hpp"
#include "server/abstract_sparse_ssp_recorder.hpp"

#include <list>
#include <vector>
#include <unordered_map>
#include <queue>

namespace flexps {

class SparseSSPController {
 public:
  SparseSSPController() = delete;
  SparseSSPController(uint32_t staleness, uint32_t speculation);

  std::list<Message> Clock(int progress, int sender, int updated_min_clock, int min_clock);

  void AddRecord(Message& msg);

  void HandleTooFastBuffer(int updated_min_clock, int min_clock, std::list<Message>& rets);

  // get_buffer's func
  std::list<Message> Pop(const int version, const int tid);
  void Push(const int version, Message& message, const int tid);
  int Size(const int version);

 private:
  uint32_t staleness_;
  uint32_t speculation_;

  // <version, <thread_id, [msg]>>
  std::unordered_map<int, std::unordered_map<int, std::list<Message>>> buffer_;

  std::vector<Message> too_fast_buffer_;

  std::unique_ptr<AbstractSparseSSPRecorder> recorder_;

};

}  // namespace flexps

