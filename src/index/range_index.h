//
// Created by adesola on 1/20/25.
//

#pragma once
#include "arrow_index.h"

namespace epoch_frame
{

    class RangeIndex : public ArrowIndex<true>
    {
      public:
        using value_type = uint64_t;
        using array_type = arrow::UInt64Array;
        explicit RangeIndex(std::shared_ptr<arrow::UInt64Array> const& array,
                            std::optional<MonotonicDirection> monotonic_direction = std::nullopt,
                            std::string const&                name                = "");
        explicit RangeIndex(std::shared_ptr<arrow::Array> const& array,
                            std::optional<MonotonicDirection>    monotonic_direction = std::nullopt,
                            std::string const&                   name                = "");

        IndexPtr Make(std::shared_ptr<arrow::Array> array) const final;
    };
} // namespace epoch_frame
