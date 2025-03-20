#pragma once
#include "epochframe/array.h"
#include "calendar/date_offsets.h"


CREATE_ENUM(EpochTimeGrouperClosedType, Left, Right);
CREATE_ENUM(EpochTimeGrouperLabelType, Left, Right);
CREATE_ENUM(EpochTimeGrouperOrigin, Epoch, Start, StartDay, EndDay, End);

namespace epochframe {
using OriginType = std::variant<DateTime, EpochTimeGrouperOrigin>;
struct TimeGrouperOptions {
    DateOffsetHandlerPtr freq;
    std::optional<std::string> key;
    EpochTimeGrouperClosedType closed{EpochTimeGrouperClosedType::Null};
    EpochTimeGrouperLabelType label{EpochTimeGrouperLabelType::Null};
    OriginType origin{EpochTimeGrouperOrigin::StartDay};
    std::optional<TimeDelta> offset{std::nullopt};
};

struct TimeBinsResult {
    std::vector<int64_t> bins;
    IndexPtr labels;
};

std::vector<int64_t> generate_bins(Array const& ax_values, Array const& bin_edges, EpochTimeGrouperClosedType closed=EpochTimeGrouperClosedType::Left);

// Resampler class
class TimeGrouper {
    public:
        TimeGrouper(const TimeGrouperOptions& options);
        TimeBinsResult get_time_bins(class DateTimeIndex const&) const;

        arrow::ChunkedArrayPtr apply(arrow::ChunkedArrayPtr const& array, std::string const& name="") const;
        arrow::ChunkedArrayPtr apply( DateTimeIndex const&) const;
    private:
        TimeGrouperOptions m_options;

        std::array<arrow::TimestampScalar, 2> get_timestamp_range_edges(DateTimeIndex const& index, DateTime const& start, DateTime const& end) const;
        std::pair<Scalar, Scalar> adjust_dates_anchored(DateTime const& first, DateTime const& last, OriginType const& origin, TickHandler const& freq) const;
        std::pair<IndexPtr, Array> adjust_bin_edges(IndexPtr binner, Array const& ax_values) const;
    };
}
