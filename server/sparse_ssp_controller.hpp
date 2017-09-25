#pragma once

#include "base/message.hpp"

#include <list>
#include <vector>
#include <unordered_map>

namespace flexps {

class SparseSSPController {
 public:
  SparseSSPController() = delete;
  SparseSSPController(uint32_t staleness, uint32_t speculation)
    : staleness_(staleness), speculation_(speculation) {}

  std::list<Message> Clock(int progress, int sender, int updated_min_clock, int min_clock);
  void AddRecord(Message& msg);

  // get_buffer's func
  std::list<Message> Pop(const int version, const int tid);
  void Push(const int version, Message& message, const int tid);
  int Size(const int version);


  // recorder's func
  bool HasConflict(const third_party::SArray<uint32_t>& paramIDs, const int begin_version,
                            const int end_version, int& forwarded_thread_id, int& forwarded_version);
  void RemoveRecord(const int version, const uint32_t tid,
                            const third_party::SArray<uint32_t>& paramIDs);
  void ClockRemoveRecord(const int version);

  int ParamSize(const int version);
  int WorkerSize(const int version);
  int TotalSize(const int version);

 private:
  uint32_t staleness_;
  uint32_t speculation_;

  // <version, <thread_id, [msg]>>
  std::unordered_map<int, std::unordered_map<int, std::list<Message>>> buffer_;

  // <version, <key, <thread_id>>>
  std::unordered_map<int, std::unordered_map<uint32_t, std::set<uint32_t>>> recorder_;

  // <thread_id, key>
  std::unordered_map<int, third_party::SArray<Key>> keys_;

  std::vector<Message> too_fast_buffer_;
};

}  // namespace flexps

