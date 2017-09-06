#include <map>
#include <vector>

namespace flexps {

class ProgressTracker {
 public:
  void Init(const std::vector<int>& tids);
  /*
   * Advance the progress.
   * Return -1 if min_clock_ does not change,
   * return min_clock_ otherwise.
   */
  int AdvanceAndGetChangedMinClock(int tid);
  int GetProgress(int tid) const;
  int GetMinClock() const;
  int GetNumThreads() const;
  bool IsUniqueMin(int tid) const;
  bool CheckThreadValid(int tid) const;

 private:
  std::map<int, int> progresses_;
  int min_clock_;
};

}  // namespace flexps
