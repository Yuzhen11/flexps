#include "server/message.hpp"
#include "server/abstract_consistency_controller.hpp"
#include "server/abstract_storage.hpp"
#include "server/progress_tracker.hpp"

#include <memory>

namespace flexps {

class ModelManager {
 public:
  ModelManager()
  void Init();
  void Clock(int tid);
  void Add(int tid, const Message& message);
  void Get(int tid, const Message& message);
 private:
  std::unique_ptr<AbstractConsistencyController> consistency_controller_;
};

}  // namespace flexps
