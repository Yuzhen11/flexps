#include <vector>

namespace flexps {

class AbstractConsistencyController {
 public:
  virtual void Clock(int tid) = 0;
  virtual void Add(int tid, const Message& message) = 0;
  virtual void Get(int tid, const Message& message) = 0;
};

}  // namespace flexps
