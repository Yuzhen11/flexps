#include "server/sparse_conflict_detector.hpp"

#include "glog/logging.h"

namespace flexps {

/* IF:
 *   NO conflict: return false
 *   ONE or SEVERAL conflict: append to the corresponding get buffer, return true
 */
bool SparseConflictDetector::ConflictInfo(const third_party::SArray<uint32_t>& paramIDs, const int begin_version,
                                          const int end_version, int& forwarded_thread_id, int& forwarded_version) {
  for (int check_version = end_version; check_version >= begin_version; check_version--) {
    for (auto& key : paramIDs) {
      auto it = recorder_[check_version].find(key);
      if (it != recorder_[check_version].end()) {
        forwarded_thread_id = *((it->second).begin());
        forwarded_version = check_version + 1;
        return true;
      }
    }
  }
  return false;
}

void SparseConflictDetector::AddRecord(const int version, const uint32_t tid, const third_party::SArray<uint32_t>& paramIDs) {
  // TODO(Ruoyu Wu): should use key in the magic.h
  for (auto& key : paramIDs) {
    recorder_[version][key].insert(tid);
  }
}

void SparseConflictDetector::RemoveRecord(const int version, const uint32_t tid, const third_party::SArray<uint32_t>& paramIDs) {
  // TODO(Ruoyu Wu): should use key in the magic.h
  CHECK(recorder_.find(version) != recorder_.end());
  for (auto& key : paramIDs) {
    CHECK(recorder_[version].find(key) != recorder_[version].end());
    CHECK(recorder_[version][key].find(tid) != recorder_[version][key].end());
    recorder_[version][key].erase(tid);
    CHECK(recorder_[version][key].size() >= 0);
    if (recorder_[version][key].size() == 0) {
      recorder_[version].erase(key);
    }
  }
}

void SparseConflictDetector::ClockRemoveRecord(const int version) { 
  recorder_.erase(version);
}

int SparseConflictDetector::ParamSize(const int version) {
  if (recorder_.find(version) == recorder_.end())
    return 0;
  return recorder_[version].size();
}

int SparseConflictDetector::WorkerSize(const int version) {
  if (recorder_.find(version) == recorder_.end())
    return 0;
  std::set<uint32_t> thread_id;
  for (auto& param_map : recorder_[version]) {
    for (auto& thread : param_map.second) {
      thread_id.insert(thread);
    }
  }
  return thread_id.size();
}

int SparseConflictDetector::TotalSize(const int version) {
  if (recorder_.find(version) == recorder_.end())
    return 0;
  int total_count = 0;
  for (auto& param_map : recorder_[version]) {
    for (auto& thread : param_map.second) {
      total_count ++;
    }
  }
  return total_count;
}

}  // namespace flexps
