#pragma once
#include <arrow/api.h>
#include "epoch_frame/aliases.h"


namespace epoch_frame {
    arrow::ArrayPtr ewm(arrow::DoubleArray const& values, int64_t minp,
    double com, bool adjust, bool ignore_na, std::shared_ptr<arrow::DoubleArray> const& deltas=nullptr, bool normalize=true);

    arrow::ArrayPtr ewmcov(arrow::DoubleArray const& input_x, int64_t minp,
    arrow::DoubleArray const& input_y, double com, bool adjust, bool ignore_na,  bool bias);

    template<typename T>
    T zsqrt(T const& values);

    extern template Array zsqrt<Array>(Array const& values);
    extern template Series zsqrt<Series>(Series const& values);
    extern template DataFrame zsqrt<DataFrame>(DataFrame const& values);
}

