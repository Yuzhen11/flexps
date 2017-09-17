#include "server/sparse_conflict_detector.hpp"

namespace flexps {

/* IF:
 *   NO conflict: return false
 *   ONE or SEVERAL conflict: append to the corresponding get buffer, return true
 */
bool SparseConflictDetector::ConflictInfo(const third_party::SArray<char>& paramIDs, const int begin_version,
                                          const int end_version) {
  auto keys = third_party::SArray<uint32_t>(paramIDs);
  for (int check_version = end_version; check_version >= begin_version; check_version--) {
    for (auto& key : keys) {
      if (recorder_[check_version].find(key) != recorder_[check_version].end())
        // conflict detected

        return true;
    }
  }
  return false;
}

void SparseConflictDetector::AddRecord(const int version, const third_party::SArray<char>& paramIDs) {
  // TODO(Ruoyu Wu): should use key in the magic.h
  auto keys = third_party::SArray<uint32_t>(paramIDs);
  for (auto& key : keys) {
    recorder_[version][key]++;
  }
}

void SparseConflictDetector::RemoveRecord(const int version, const third_party::SArray<char>& paramIDs) {
  // TODO(Ruoyu Wu): should use key in the magic.h
  auto keys = third_party::SArray<uint32_t>(paramIDs);
  for (auto& key : keys) {
    recorder_[version][key]--;
    CHECK(recorder_[version][key] >= 0);
    if (recorder_[version][key] == 0) {
      recorder_[version].erase(key);
    }
  }
}

void SparseConflictDetector::ClockRemoveClock(const int version) { recorder_.erase(version); }

int SparseConflictDetector::RecorderSize(const int version) {
  int total_count = 0;
  for (auto& param_map : recorder_[version]) {
    total_count += param_map.second();
  }
  return total_count;
}

}  // namespace flexps
