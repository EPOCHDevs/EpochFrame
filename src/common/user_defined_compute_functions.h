#pragma once
#include <arrow/api.h>
#include "epochframe/aliases.h"


namespace epochframe {
    arrow::ArrayPtr ewm(arrow::DoubleArray const& values, int64_t minp,
    double com, bool adjust, bool ignore_na, std::shared_ptr<arrow::DoubleArray> const& deltas=nullptr, bool normalize=true);

    arrow::ArrayPtr ewmcov(arrow::DoubleArray const& input_x, int64_t minp,
    arrow::DoubleArray const& input_y, double com, bool adjust, bool ignore_na,  bool bias);

    FrameOrSeries zsqrt(FrameOrSeries const& values);
}

