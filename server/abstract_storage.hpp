#include "server/message.hpp"

namespace flexps {

class AbstractStorage {
 public:
  virtual void Add(const Message& message) = 0;
  virtual void Get(const Message& message) = 0;
  virtual void FinishIter() = 0;
};

}  // namespace flexps
