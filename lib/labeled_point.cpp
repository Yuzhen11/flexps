#include <vector>
#include<utility>

namespace flexps {

template <typename T>
struct FeaValPair {
    FeaValPair() = default;
    FeaValPair(int fea, T val) : fea(fea), val(val) {}
    int fea;
    T val;
};

template <typename T, typename U>
struct LabeledPoint {
    LabeledPoint() = default;
    LabeledPoint(T& x, U& y) : x(x), y(y) {}
    LabeledPoint(T&& x, U&& y) : x(std::move(x)), y(std::move(y)) {}

    T x;
    U y;
};

// extends LabeledPoint, using Vector to represent features
template <typename FeatureT, typename LabelT>
class LabeledPointHObj : public LabeledPoint<std::vector<FeaValPair<FeatureT>>, LabelT> {
    public:
        using FeatureV = std::vector<FeaValPair<FeatureT>>;

        // constructors
        LabeledPointHObj() : LabeledPoint<FeatureV, LabelT>() {}
        explicit LabeledPointHObj(int feature_num) : LabeledPoint<FeatureV, LabelT>() { this->x = FeatureV(feature_num); }
        LabeledPointHObj(FeatureV& x, LabelT& y) : LabeledPoint<FeatureV, LabelT>(x, y) {}
        LabeledPointHObj(FeatureV&& x, LabelT&& y) : LabeledPoint<FeatureV, LabelT>(x, y) {}
};  // LabeledPointHObj

}  // namespace flexps