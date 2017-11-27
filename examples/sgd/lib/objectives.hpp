#pragma once

namespace flexps {

class Objective {  // TODO may wrap model and paramters
 public:
  explicit Objective(int num_params) : num_params_(num_params) {}
  virtual void get_gradient(const std::vector<std::pair<std::vector<std::pair<int, float>>, float>>& batch,
                            const std::vector<Key>& keys, const std::vector<float>& params,
                            std::vector<float>* delta) = 0;
  virtual float get_loss(const std::vector<std::pair<std::vector<std::pair<int, float>>, float>>& data_store,
                         const std::vector<float>& model) = 0;

  virtual void process_keys(std::vector<Key>* keys) {
    keys->push_back(num_params_ - 1);  // add key for bias
  }

  virtual void all_keys(std::vector<Key>* keys) {
    keys->resize(num_params_);
    for (int i = 0; i < num_params_; ++i)
      keys->at(i) = i;
  }

 protected:
  int num_params_ = 0;
};

class SigmoidObjective : public Objective {
 public:
  explicit SigmoidObjective(int num_params) : Objective(num_params){};

  void get_gradient(const std::vector<std::pair<std::vector<std::pair<int, float>>, float>>& batch,
                    const std::vector<Key>& keys, const std::vector<float>& params, std::vector<float>* delta) {
    if (batch.empty())
      return;
    for (auto data : batch) {  // iterate over the data in the batch
      auto& x = data.first;
      float y = data.second;
      if (y < 0)
        y = 0.;
      float pred_y = 0.0;
      int i = 0;
      for (auto field : x) {
        while (keys[i] < field.first)
          i += 1;                            // first:fea
        pred_y += params[i] * field.second;  // second:val
      }
      pred_y += params.back();  // intercept
      pred_y = 1. / (1. + exp(-1 * pred_y));
      i = 0;
      for (auto field : x) {
        while (keys[i] < field.first)
          i += 1;
        delta->at(i) += field.second * (pred_y - y);
      }
      delta->back() += pred_y - y;
    }
    int batch_size = batch.size();
    for (auto& d : *delta) {
      d /= static_cast<float>(batch_size);
    }
  }

  float get_loss(const std::vector<std::pair<std::vector<std::pair<int, float>>, float>>& data_store,
                 const std::vector<float>& model) {
    int count = 0;
    float loss = 0.0f;
    for (int i = 0; i < data_store.size(); ++i) {
      auto& data = data_store[i];
      count += 1;

      auto& x = data.first;
      float y = data.second;
      if (y < 0)
        y = 0.;
      float pred_y = 0.0f;
      for (auto& field : x) {
        pred_y += model[field.first] * field.second;
      }
      pred_y += model.back();  // intercept
      pred_y = 1. / (1. + exp(-pred_y));
      if (y == 0) {
        loss += -log(1. - pred_y);
      } else {  // y == 1
        loss += -log(pred_y);
      }
    }
    if (count == 0)
      return 0.;
    loss /= static_cast<float>(count);
    return loss;
  }
};

class LassoObjective : public Objective {
 public:
  explicit LassoObjective(int num_params) : Objective(num_params){};
  LassoObjective(int num_params, float lambda) : Objective(num_params), lambda_(lambda){};

  void get_gradient(const std::vector<std::pair<std::vector<std::pair<int, float>>, float>>& batch,
                    const std::vector<Key>& keys, const std::vector<float>& params, std::vector<float>* delta) {
    if (batch.empty())
      return;
    // 1. Calculate the sum of gradients
    for (auto data : batch) {  // iterate over the data in the batch
      auto& x = data.first;
      float y = data.second;
      float pred_y = 0.0;
      int i = 0;
      for (auto field : x) {
        while (keys[i] < field.first)
          i += 1;  // first=fea, second=val
        pred_y += params[i] * field.second;
      }
      pred_y += params.back();  // intercept
      i = 0;
      for (auto field : x) {
        while (keys[i] < field.first)
          i += 1;
        delta->at(i) += field.second * (pred_y - y);
      }
      delta->back() += pred_y - y;
    }
    int batch_size = batch.size();

    // 2. Get average and add regularization
    delta->back() /= static_cast<float>(batch_size);
    for (int i = 0; i < keys.size() - 1; ++i) {
      delta->at(i) /= static_cast<float>(batch_size);
      if (params[i] == 0.)
        continue;
      delta->at(i) +=
          (params[i] > 0) ? lambda_ : -lambda_;  // TODO(Tatiana): this is no good, but consistent with Multiverso
    }
  }

  float get_loss(const std::vector<std::pair<std::vector<std::pair<int, float>>, float>>& data_store,
                 const std::vector<float>& model) {
    // 1. Calculate MSE on samples
    int count = 0;
    float loss = 0.0f;
    for (int i = 0; i < data_store.size(); ++i) {
      // while (data_iterator.has_next()) {
      auto& data = data_store[i];
      count += 1;

      auto& x = data.first;
      float y = data.second;
      float pred_y = 0.0f;
      for (auto& field : x) {
        pred_y += model[field.first] * field.second;
      }
      pred_y += model.back();  // intercept
      float diff = pred_y - y;
      loss += diff * diff;
    }
    if (count != 0) {
      loss /= static_cast<float>(count);
    }

    // 2. Calculate regularization penalty
    float regularization = 0.;
    for (float param : model) {
      regularization += fabs(param);
    }
    loss += regularization * lambda_;

    return loss;
  }

  inline void set_lambda(float lambda) { lambda_ = lambda; }

 private:
  float lambda_ = 0;  // l1 regularizer
};

class SVMObjective : public Objective {
 public:
  explicit SVMObjective(int num_params) : Objective(num_params){};
  SVMObjective(int num_params, float lambda) : Objective(num_params), lambda_(lambda){};

  void get_gradient(const std::vector<std::pair<std::vector<std::pair<int, float>>, float>>& batch,
                    const std::vector<Key>& keys, const std::vector<float>& params, std::vector<float>* delta) {
    if (batch.empty())
      return;
    // 1. Hinge loss gradients
    for (auto data : batch) {  // iterate over the data in the batch
      auto& x = data.first;
      float y = data.second;
      float pred_y = 0.0;
      int i = 0;
      for (auto field : x) {
        while (keys[i] < field.first)
          i += 1;  // first=fea, second=val
        pred_y += params[i] * field.second;
      }
      pred_y += params.back();  // intercept
      if (y * pred_y < 1) {     // in soft margin
        i = 0;
        for (auto field : x) {
          while (keys[i] < field.first)
            i += 1;
          delta->at(i) -= field.second * y;
        }
        delta->back() -= y;
      }
    }

    int batch_size = batch.size();
    // 2. ||w||^2 gradients
    for (int i = 0; i < keys.size() - 1; ++i) {  // omit bias
      delta->at(i) /= static_cast<float>(batch_size);
      delta->at(i) += lambda_ * params[i];
    }
  }

  float get_loss(const std::vector<std::pair<std::vector<std::pair<int, float>>, float>>& data_store,
                 const std::vector<float>& model) {
    // 1. Calculate hinge loss
    int count = 0;
    float loss = 0.0f;
    for (int i = 0; i < data_store.size(); ++i) {
      // while (data_iterator.has_next()) {
      auto& data = data_store[i];
      count += 1;

      auto& x = data.first;
      float y = data.second;
      float pred_y = 0.0f;
      for (auto& field : x) {
        pred_y += model[field.first] * field.second;
      }
      pred_y += model.back();  // intercept
      loss += std::max(0., 1. - y * pred_y);
    }
    if (count != 0) {
      loss /= static_cast<float>(count);
    }

    // 2. Calculate ||w||^2
    float w_2 = 0.;
    for (float param : model) {
      w_2 += param * param;
    }
    loss += w_2 * lambda_;

    return loss;
  }

  inline void set_lambda(float lambda) { lambda_ = lambda; }

 private:
  float lambda_ = 0;  // hinge loss factor
};

}  // namespace flexps
