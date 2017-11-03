#include <vector>
#include "base/third_party/sarray.h"

namespace flexps {

using Key = uint32_t;

/*
 * BatchDataSampler: Sample the data in batch
 * Can work on the whole datastore
 *
 * Usage:
 *   batch_data_sampler.prepare_next_batch();
 *   for (auto data : get_data_ptrs) {
 *      ...
 *   }
 */
template<typename T>
class BatchDataSampler {
   public:
    BatchDataSampler() = delete;
    BatchDataSampler(std::vector<T>& data, int batch_size) {
      batch_size_ = batch_size;
      data_ = data;
      batch_data_.resize(batch_size);
    }

    bool empty() {
        return data_.empty();
    }
    int get_batch_size() const {
        return batch_size_;
    }
    void random_start_point() {
        cur_pos = rand() % data_.size();
    }
    /*
     * \return keys in next_batch
     * store next batch data pointer in batch_data_, doesn't own the data
     */
    third_party::SArray<Key> prepare_next_batch() {
        if (empty())
            return {};
        third_party::SArray<Key> keys;
        std::set<Key> s;
        for (int i = 0; i < batch_size_; ++ i) {
            auto& data = data_[cur_pos++];
            batch_data_[i] = const_cast<T*>(&data);

            if (cur_pos == data_.size())
              cur_pos = 0;
        }
        for (auto data : batch_data_) {
            for (auto field : data->first) {
              s.insert(field.first);
            }
        }
        for (auto k : s) {
          keys.push_back(k);
        }
        return keys;
    }
    const std::vector<T*>& get_data_ptrs() {
        if (empty())
            return empty_batch_;
        else
            return batch_data_;  // batch_data_ should have been prepared
    }
   private:
    std::vector<T> data_;
    int cur_pos = 0;
    int batch_size_;
    std::vector<T*> batch_data_;
    std::vector<T*> empty_batch_;  // Only for empty batch usage
};

}  // namespace flexps