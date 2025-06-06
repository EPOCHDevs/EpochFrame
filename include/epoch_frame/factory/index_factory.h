//
// Created by adesola on 1/20/25.
//

#pragma once

#include <vector>
#include <methods/temporal.h>

#include "epoch_frame/aliases.h"
#include "arrow/scalar.h"
#include "epoch_frame/enums.h"
#include "date_time/date_offsets.h"


namespace epoch_frame::factory::index {
    IndexPtr make_range(std::vector<uint64_t> const&, MonotonicDirection monotonic_direction);

    IndexPtr from_range(int64_t start, int64_t stop, int64_t step = 1);

    IndexPtr from_range(int64_t stop);

    IndexPtr make_object_index(const std::vector<std::string> &data);

    IndexPtr make_object_index(const std::vector<arrow::ScalarPtr> &data);

    struct DateRangeOptions {
        arrow::TimestampScalar start;
        std::optional<arrow::TimestampScalar> end{};
        std::optional<int64_t> periods{};
        DateOffsetHandlerPtr offset;
        std::string tz{""};
        AmbiguousTimeHandling ambiguous{AmbiguousTimeHandling::RAISE};
        NonexistentTimeHandling nonexistent{NonexistentTimeHandling::RAISE};
    };
    IndexPtr date_range(DateRangeOptions const&);

    IndexPtr make_datetime_index(std::vector<int64_t> const& timestamps, std::string const& name="", std::string const& tz="");
    IndexPtr make_datetime_index(std::vector<DateTime> const& timestamps, std::string const& name="", std::string const& tz="");
    IndexPtr make_datetime_index(std::vector<arrow::TimestampScalar> const& timestamps, std::string const& name="", std::string const& tz="");
    IndexPtr make_index(arrow::ArrayPtr const& index_array, std::optional<MonotonicDirection> monotonic_direction, std::string const& name);

    IndexPtr make_index(arrow::ChunkedArrayPtr const& index_array, std::optional<MonotonicDirection> monotonic_direction, std::string const& name);
    IndexPtr make_empty_index(arrow::DataTypePtr const& index_array, std::optional<MonotonicDirection> monotonic_direction, std::string const& name);

}
