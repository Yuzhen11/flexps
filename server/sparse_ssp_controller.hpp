#pragma once

#include "server/abstract_sparse_ssp_controller.hpp"
#include "server/sparse_conflict_detector.hpp"
#include "server/sparse_pending_buffer.hpp"


namespace flexps {

class SparseSSPController : public AbstractSparseSSPController {
 public:
  SparseSSPController(uint32_t staleness, uint32_t speculation, 
                      std::unique_ptr<AbstractPendingBuffer>&& get_buffer,
                      std::unique_ptr<AbstractConflictDetector>&& detector)
    : staleness_(staleness), speculation_(speculation) {}

  virtual std::list<Message> UnblockRequests(int progress, int sender, int updated_min_clock, int min_clock) override;
  virtual void AddRecord(Message& msg) override;

  // get_buffer's func
  virtual std::list<Message> Pop(const int version, const int tid = -1) override;
  virtual std::list<Message>& Get(const int version, const int tid = -1) override;
  virtual void Push(const int version, Message& message, const int tid = -1) override;
  virtual int Size(const int version) override;


  // recorder's func
  virtual bool ConflictInfo(const third_party::SArray<uint32_t>& paramIDs, const int begin_version,
                            const int end_version, int& forwarded_thread_id, int& forwarded_version) override;
  virtual void AddRecord(const int version, const uint32_t tid, const third_party::SArray<uint32_t>& paramIDs) override;
  virtual void RemoveRecord(const int version, const uint32_t tid,
                            const third_party::SArray<uint32_t>& paramIDs) override;
  virtual void ClockRemoveRecord(const int version) override;

  int ParamSize(const int version);
  int WorkerSize(const int version);
  int TotalSize(const int version);

 private:
  uint32_t staleness_;
  uint32_t speculation_;

  std::unordered_map<int, std::unordered_map<int, std::list<Message>>> buffer_;
  std::unordered_map<int, std::unordered_map<uint32_t, std::set<uint32_t>>> recorder_;

  std::vector<Message> too_fast_buffer_;
};



}  // namespace flexps

