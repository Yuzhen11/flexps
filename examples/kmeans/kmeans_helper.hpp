#include <utility>
#include <vector>

namespace flexps {

template <typename T>
float dist(T& point1, T& point2, int num_features) {
  std::vector<float> diff(num_features);
  auto& x1 = point1.first;
  auto& x2 = point2.first;

  for (auto field : x1)
    diff[field.first] = field.second;  // first:fea, second:val

  for (auto field : x2)
    diff[field.first] = field.second;  // first:fea, second:val

  return std::inner_product(diff.begin(), diff.end(), diff.begin(), 0.0);
}

template <typename T>
void random_init(int K, int num_features, const std::vector<T>& data, std::vector<std::vector<float>>& params) {
  // LOG(INFO)<<"inside random";
  std::vector<int> prohibited_indexes;
  int index;
  for (int i = 0; i < K; i++) {
    while (true) {
      srand(time(NULL));
      index = rand() % data.size();
      if (find(prohibited_indexes.begin(), prohibited_indexes.end(), index) ==
          prohibited_indexes.end())  // not found, this index can be used
      {
        prohibited_indexes.push_back(index);
        auto& x = data[index].first;
        for (auto field : x)
          params[i][field.first] = field.second;  // first:fea, second:val
        break;
      }
    }
    params[K][i] += 1;
  }
}

// return ID of cluster whose center is the nearest (uses euclidean distance), and the distance
std::pair<int, float> get_nearest_center(const std::pair<std::vector<std::pair<int, float>>, float>& point, int K,
                                         const std::vector<std::vector<float>>& params, int num_features) {
  float square_dist, min_square_dist = std::numeric_limits<float>::max();
  int id_cluster_center = -1;
  auto& x = point.first;

  for (int i = 0; i < K; i++)  // calculate the dist between point and clusters[i]
  {
    std::vector<float> diff = params[i];

    for (auto field : x)
      diff[field.first] -= field.second;  // first:fea, second:val

    square_dist = std::inner_product(diff.begin(), diff.end(), diff.begin(), 0.0);
    if (square_dist < min_square_dist) {
      min_square_dist = square_dist;
      id_cluster_center = i;
    }
  }
  return std::make_pair(id_cluster_center, min_square_dist);
}

template <typename T>
void kmeans_plus_plus_init(int K, int num_features, const std::vector<T>& data,
                           std::vector<std::vector<float>>& params) {
  // LOG(INFO)<<"inside kmeans ++";
  auto X = data;
  std::vector<float> dist(X.size());
  int index;

  index = rand() % X.size();
  auto& x = X[index].first;
  for (auto field : x)
    params[0][field.first] = field.second;  // first:fea, second:val

  params[K][0] += 1;
  X.erase(X.begin() + index);

  float sum;
  int id_nearest_center;
  for (int i = 1; i < K; i++) {
    sum = 0;
    for (int j = 0; j < X.size(); j++) {
      dist[j] = get_nearest_center(X[j], i, params, num_features).second;
      sum += dist[j];
    }

    sum = sum * rand() / (RAND_MAX - 1.);

    for (int j = 0; j < X.size(); j++) {
      sum -= dist[j];
      if (sum > 0)
        continue;

      auto& x = X[j].first;
      for (auto field : x)
        params[i][field.first] = field.second;  // first:fea, second:val

      X.erase(X.begin() + j);
      break;
    }
    params[K][i] += 1;
  }
}

template <typename T>
void kmeans_parallel_init(int K, int num_features, const std::vector<T>& data,
                          std::vector<std::vector<float>>& params) {
  // LOG(INFO)<<"inside kmeans ||";
  int index;
  std::vector<T> C;
  // std::vector<LabeledPointHObj<float, int, true>> C;
  auto X = data;
  index = rand() % X.size();
  C.push_back(X[index]);
  X.erase(X.begin() + index);
  float square_dist, min_dist;
  /*float sum_square_dist = 0;  // original K-Means|| algorithms
  for(int i = 0; i < X.size(); i++)
      sum_square_dist += dist(C[0], X[i], num_features);
  int sample_time = log(sum_square_dist), l = 2;*/
  int sample_time = 5, l = 2;  // empiric value, sampe_time: time of sampling   l: oversample coefficient

  for (int i = 0; i < sample_time; i++) {
    // compute d^2 for each x_i
    std::vector<float> psi(X.size());

    for (int j = 0; j < X.size(); j++) {
      min_dist = std::numeric_limits<float>::max();
      for (int k = 0; k < C.size(); k++) {
        square_dist = dist(X[j], C[k], num_features);
        if (square_dist < min_dist)
          min_dist = square_dist;
      }

      psi[j] = min_dist;
    }

    float phi = 0;
    for (int i = 0; i < psi.size(); i++)
      phi += psi[i];

    // do the drawings
    for (int i = 0; i < psi.size(); i++) {
      float p_x = l * psi[i] / phi;

      if (p_x >= rand() / (RAND_MAX - 1.)) {
        C.push_back(X[i]);
        X.erase(X.begin() + i);
      }
    }
  }

  std::vector<float> w(C.size());  // by default all are zero
  for (int i = 0; i < X.size(); i++) {
    min_dist = std::numeric_limits<float>::max();
    for (int j = 0; j < C.size(); j++) {
      square_dist = dist(X[i], C[j], num_features);
      if (square_dist < min_dist) {
        min_dist = square_dist;
        index = j;
      }
    }
    // we found the minimum index, so we increment corresp. weight
    w[index]++;
  }

  // repeat kmeans++ on C
  index = rand() % C.size();
  auto& x = C[index].first;
  for (auto field : x)
    params[0][field.first] = field.second;  // first:fea, second:val

  params[K][0] += 1;
  C.erase(C.begin() + index);

  float sum;
  for (int i = 1; i < K; i++) {
    sum = 0;
    std::vector<float> dist(C.size());
    for (int j = 0; j < C.size(); j++) {
      dist[j] = get_nearest_center(C[j], i, params, num_features).second;
      sum += dist[j];
    }

    sum = sum * rand() / (RAND_MAX - 1.);

    for (int j = 0; j < C.size(); j++) {
      sum -= dist[j];
      if (sum > 0)
        continue;

      auto& x = C[j].first;
      for (auto field : x)
        params[i][field.first] = field.second;  // first:fea, second:val

      C.erase(C.begin() + j);
      break;
    }
    params[K][i] += 1;
  }
}

template <typename T>
void init_centers(int K, int num_features, std::vector<T> data, std::vector<std::vector<float>>& params,
                  std::string init_mode) {
  if (init_mode == "random")  // K-means: choose K distinct values for the centers of the clusters randomly
    random_init(K, num_features, data, params);
  else if (init_mode == "kmeans++")  // K-means++
    kmeans_plus_plus_init(K, num_features, data, params);
  else if (init_mode ==
           "kmeans_parallel") {  // K-means||, reference: http://theory.stanford.edu/~sergei/papers/vldb12-kmpar.pdf
    kmeans_parallel_init(K, num_features, data, params);
  }
}

// test the Sum of Square Error of the model
template <typename T>
void test_error(const std::vector<std::vector<float>>& params, const std::vector<T>& data, int iter, int K,
                int num_features, int cluster_id) {
  float sum = 0;  // sum of square error
  std::pair<int, float> id_dist;
  std::vector<int> count(K);

  for (int i = 0; i < data.size(); i++) {
    // get next data
    id_dist = get_nearest_center(data[i], K, params, num_features);
    sum += id_dist.second;
    count[id_dist.first]++;
  }

  LOG(INFO) << "Worker " + std::to_string(cluster_id) + ", iter " + std::to_string(iter)
            << ": Within Set Sum of Squared Errors = " << std::to_string(sum);
  for (int i = 0; i < K; i++)  // for tuning learning rate
    LOG(INFO) << "Worker " + std::to_string(cluster_id) + ", count" + std::to_string(i) + ": " +
                     std::to_string(count[i]);
}
}
