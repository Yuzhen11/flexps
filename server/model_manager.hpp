#include <memory>
#include "server/abstract_model.hpp"
#include "server/message.hpp"

namespace flexps {

class ServerModel : AbstractModel {
 public:
  serverModel() void Init();
  virtual void Clock(int tid) override;
  virtual void Add(int tid, const Message& message) override;
  virtual void Get(int tid, const Message& message) override;

 private:
  std::unique_ptr<AbstractConsistencyController> consistency_controller_;
};

}  // namespace flexps
