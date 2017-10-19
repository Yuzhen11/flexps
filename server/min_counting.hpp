#include "server/abstract_counting.hpp"

#include <vector>
#include <functional>
#include <chrono>

namespace flexps {

class MinCounting : public AbstractCounting {
public:
  explicit MinCounting(int num_hash, int num_bin);

  virtual void Add(const third_party::SArray<uint32_t> keys) override;
  virtual void Remove(const third_party::SArray<uint32_t> keys) override;
  virtual void RemoveAll() override;
  virtual uint32_t Count(const uint32_t& key) override;

  void HashFuncResult(const uint32_t key, std::vector<uint32_t>& hashes);
  int HashFuncTimer(int number);

private:
  // TODO(Ruoyu Wu): More efficient data structure?
  std::vector<std::vector<uint32_t>> table_;

  const int NUM_HASH_;
  const int NUM_BIN_;
};

}  // namespace flexps
