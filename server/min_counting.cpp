#include "server/min_counting.hpp"
#include "glog/logging.h"

#include <boost/functional/hash.hpp>
#include <boost/random/mersenne_twister.hpp>
#include <boost/random/uniform_int.hpp>
#include <boost/random/uniform_real.hpp>
#include <boost/random/variate_generator.hpp>

#include <limits>

namespace flexps {

MinCounting::MinCounting(int num_hash, int num_bin) : NUM_HASH_(num_hash), NUM_BIN_(num_bin) {
  table_.resize(NUM_HASH_);
  for (auto& hash_table : table_) {
    hash_table.resize(NUM_BIN_, 0);
  }
}

void MinCounting::Add(const third_party::SArray<uint32_t> keys) {
  std::vector<uint32_t> hashes(NUM_HASH_);
  for (auto& key : keys) {
    HashFuncResult(key, hashes);
    DCHECK(hashes.size() == NUM_HASH_);
    for (int hash_table_index = 0; hash_table_index < NUM_HASH_; hash_table_index ++) {
      table_[hash_table_index][hashes[hash_table_index]] ++;
    }
  }
}

// TODO(Ruoyu Wu): not sure if the hash func will be the same
void MinCounting::Remove(const third_party::SArray<uint32_t> keys) {
  std::vector<uint32_t> hashes(NUM_HASH_);
  for (auto& key : keys) {
    HashFuncResult(key, hashes);
    DCHECK(hashes.size() == NUM_HASH_);
    for (int hash_table_index = 0; hash_table_index < NUM_HASH_; hash_table_index ++) {
      DCHECK(table_[hash_table_index][hashes[hash_table_index]] > 0);
      table_[hash_table_index][hashes[hash_table_index]] --;
    }
  }
}

void MinCounting::RemoveAll() { 
  for (auto& hash_table : table_) {
    std:fill(hash_table.begin(), hash_table.end(), 0);
  }
}

uint32_t MinCounting::Count(const uint32_t& key) {
  uint32_t min_count = std::numeric_limits<uint32_t>::max();
  std::vector<uint32_t> hashes(NUM_HASH_);
  HashFuncResult(key, hashes);

  for (int hash_table_index = 0; hash_table_index < NUM_HASH_; hash_table_index ++) {
    if (min_count > table_[hash_table_index][hashes[hash_table_index]]) {
      min_count = table_[hash_table_index][hashes[hash_table_index]];
    }
  }

  return min_count;
}

// TODO(Ruoyu Wu): can be bottleneck according to timer below
void MinCounting::HashFuncResult(const uint32_t key, std::vector<uint32_t>& hashes) {
  DCHECK(hashes.size() == NUM_HASH_);

  boost::hash<uint32_t> int_hash;
  boost::mt19937 randGen(int_hash(key));
  boost::uniform_int<> numrange(0, NUM_BIN_ - 1);
  boost::variate_generator< boost::mt19937&, boost::uniform_int<> > GetRand(randGen, numrange);

  for (int hash_table_index = 0; hash_table_index < NUM_HASH_; hash_table_index ++) {
    hashes[hash_table_index] = GetRand();
  }
}

int MinCounting::HashFuncTimer(int number) {
  std::vector<uint32_t> hashes(NUM_HASH_);

  auto start_time = std::chrono::steady_clock::now();

  for (int i = 0; i < number; i ++) {
    int my_birth = 19960223;
    HashFuncResult(my_birth, hashes);
  }

  auto end_time = std::chrono::steady_clock::now();
  auto total_time = std::chrono::duration_cast<std::chrono::milliseconds>(end_time - start_time).count();

  return total_time;
}

}  // namespace flexps
