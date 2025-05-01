#pragma once

#include "epoch_frame/series.h"
#include "epoch_frame/scalar.h"
#include <variant>

namespace epoch_frame {
    class SeriesOrScalar {
    public:
        explicit SeriesOrScalar(Series series) : series_or_scalar_(std::move(series)) {}
        explicit SeriesOrScalar(Scalar scalar) : series_or_scalar_(std::move(scalar)) {}
        SeriesOrScalar(IndexPtr index, arrow::ChunkedArrayPtr array) : series_or_scalar_(Series(std::move(index), std::move(array))) {}

        bool is_series() const {
            return std::holds_alternative<Series>(series_or_scalar_);
        }

        bool is_scalar() const {
            return std::holds_alternative<Scalar>(series_or_scalar_);
        }

        Series series() const {
            return std::get<Series>(series_or_scalar_);
        }

        Scalar scalar() const {
            return std::get<Scalar>(series_or_scalar_);
        }

        template<typename T>
        T as() const {
            return std::get<T>(series_or_scalar_);
        }


    private:
        std::variant<Series, Scalar> series_or_scalar_;
    };
}
