#pragma once

namespace flexps {

struct OptimizerConfig {
  int num_iters = 10;
  int num_features = 100;
  int num_workers_per_node = 3;
  int kTableId = 0;
  float alpha = 0.1;
  int batch_size = 10;
  int learning_rate_decay = 10;
};

class Optimizer {
 public:
  Optimizer(Objective* objective, int report_interval) : objective_(objective), report_interval_(report_interval) {}

  virtual void train(const Info& info,
                     const std::vector<std::pair<std::vector<std::pair<int, float>>, float>>& data_store,
                     const OptimizerConfig& config, int iter_offset = 0) = 0;

 protected:
  Objective* objective_;
  int report_interval_ = 0;
};

class SGDOptimizer : Optimizer {
 public:
  SGDOptimizer(Objective* objective, int report_interval) : Optimizer(objective, report_interval) {}

  void train(const Info& info, const std::vector<std::pair<std::vector<std::pair<int, float>>, float>>& data_store,
             const OptimizerConfig& config, int iter_offset = 0) override {
    // Train Now
    auto table = info.CreateKVClientTable<float>(config.kTableId);
    std::vector<Key> keys(config.num_features + 1);  // +1 for bias
    std::iota(keys.begin(), keys.end(), 0);
    std::vector<float> params(keys.size(), 0);
    std::vector<float> deltas(keys.size(), 0);

    for (int iter = iter_offset; iter < config.num_iters + iter_offset; ++iter) {
      table.Get(keys, &params);
      CHECK_EQ(keys.size(), params.size());

      float alpha = config.alpha / (iter / config.learning_rate_decay + 1);
      alpha = std::max(1e-5f, alpha);
      srand(time(0));
      int startpt = rand() % data_store.size();
      std::vector<std::pair<std::vector<std::pair<int, float>>, float>> batch_data;
      for (int i = 0; i < config.batch_size / config.num_workers_per_node; ++i) {
        if (startpt == data_store.size())
          startpt = rand() % data_store.size();
        batch_data.push_back(data_store[startpt]);
        startpt += 1;
      }

      update(batch_data, params, deltas, alpha, keys);

      // Report loss on training samples

      if (report_interval_ != 0 && (iter + 1) % report_interval_ == 0) {
        if (info.worker_id == 0) {  // let the cluster leader do the report
          auto loss = objective_->get_loss(data_store, params);
          LOG(INFO) << ": Iter, Loss: " << iter << " , " << loss;
        }
      }
      table.Add(keys, deltas);
    }
  }

 private:
  void update(const std::vector<std::pair<std::vector<std::pair<int, float>>, float>>& batch_data,
              const std::vector<float>& params, std::vector<float>& deltas, float alpha, std::vector<Key>& keys) {
    objective_->get_gradient(batch_data, keys, params, &deltas);

    for (auto& d : deltas) {
      d *= -alpha;
    }
  }
};

}  // namespace flexps
