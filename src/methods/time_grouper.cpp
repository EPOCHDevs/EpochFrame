#include "time_grouper.h"
#include "common/asserts.h"
#include "common/python_utils.h"
#include "factory/index_factory.h"
#include "index/datetime_index.h"
#include <execution>

namespace epoch_frame
{
    std::vector<int64_t> generate_bins(Array const& values, Array const& binner,
                                       EpochTimeGrouperClosedType closed)
    {
        auto lenidx = values.length();
        auto lenbin = binner.length();

        AssertFalseFromFormat(lenidx <= 0 || lenbin <= 0,
                              "Invalid length for values or for binner");
        AssertFalseFromFormat(values[0] < binner[0], "Values falls before first bin");
        AssertFalseFromFormat(values[-1] > binner[-1], "Values falls after last bin");

        std::vector<int64_t> bins(lenbin - 1);
        int64_t              j  = 0;
        int64_t              bc = 0;

        if (closed == EpochTimeGrouperClosedType::Right)
        {
            for (int64_t i = 0; i < lenbin - 1; ++i)
            {
                auto r_bin = binner[i + 1];
                while (j < lenidx && values[j] <= r_bin)
                    ++j;
                bins[bc] = j;
                ++bc;
            }
        }
        else
        {
            for (int64_t i = 0; i < lenbin - 1; ++i)
            {
                auto r_bin = binner[i + 1];
                while (j < lenidx && values[j] < r_bin)
                    ++j;
                bins[bc] = j;
                ++bc;
            }
        }
        return bins;
    }

    TimeGrouper::TimeGrouper(const TimeGrouperOptions& options) : m_options(options)
    {
        AssertFromStream(m_options.freq, "Frequency must be set");

        bool origin_is_value = std::holds_alternative<DateTime>(m_options.origin);

        if (m_options.freq->is_end() || m_options.freq->type() == EpochOffsetType::Week)
        {
            if (m_options.closed == EpochTimeGrouperClosedType::Null)
            {
                m_options.closed = EpochTimeGrouperClosedType::Right;
            }
            if (m_options.label == EpochTimeGrouperLabelType::Null)
            {
                m_options.label = EpochTimeGrouperLabelType::Right;
            }
        }
        else
        {
            if (!origin_is_value)
            {
                auto origin = std::get<EpochTimeGrouperOrigin>(m_options.origin);
                if (origin == EpochTimeGrouperOrigin::End ||
                    origin == EpochTimeGrouperOrigin::EndDay)
                {
                    if (m_options.closed == EpochTimeGrouperClosedType::Null)
                    {
                        m_options.closed = EpochTimeGrouperClosedType::Right;
                    }
                    if (m_options.label == EpochTimeGrouperLabelType::Null)
                    {
                        m_options.label = EpochTimeGrouperLabelType::Right;
                    }
                }
            }
            else
            {
                if (m_options.closed == EpochTimeGrouperClosedType::Null)
                {
                    m_options.closed = EpochTimeGrouperClosedType::Left;
                }
                if (m_options.label == EpochTimeGrouperLabelType::Null)
                {
                    m_options.label = EpochTimeGrouperLabelType::Left;
                }
            }
        }
    }

    TimeBinsResult TimeGrouper::get_time_bins(DateTimeIndex const& index) const
    {
        if (index.size() == 0)
        {
            auto binner = index.Make(Array{index.dtype()}.value());
            return {std::vector<int64_t>{}, binner};
        }

        auto ax_array = index.array();

        auto [first, last] =
            get_timestamp_range_edges(index, ax_array[0].to_datetime(), ax_array[-1].to_datetime());
        IndexPtr binner = factory::index::date_range(factory::index::DateRangeOptions{
            .start       = first,
            .end         = last,
            .offset      = m_options.freq,
            .tz          = index.tz(),
            .ambiguous   = AmbiguousTimeHandling::EARLIEST,
            .nonexistent = NonexistentTimeHandling::SHIFT_FORWARD});

        auto  labels = binner;
        Array bin_edges;
        std::tie(binner, bin_edges) = adjust_bin_edges(binner, index.array());

        auto bins = generate_bins(index.array(), bin_edges, m_options.closed);

        if (m_options.closed == EpochTimeGrouperClosedType::Right)
        {
            labels = binner;
            if (m_options.label == EpochTimeGrouperLabelType::Right)
            {
                labels = labels->iloc({1});
            }
        }
        else if (m_options.label == EpochTimeGrouperLabelType::Right)
        {
            labels = binner->iloc({1});
        }

        if (bins.size() < labels->size())
        {
            labels = labels->iloc({.stop = bins.size()});
        }

        return {bins, labels};
    }

    arrow::ChunkedArrayPtr TimeGrouper::apply(arrow::ChunkedArrayPtr const& array,
                                              std::string const&            name) const
    {
        auto contiguous_array = factory::array::make_contiguous_array(array);
        return apply(DateTimeIndex(contiguous_array, name));
    }

    arrow::ChunkedArrayPtr TimeGrouper::apply(DateTimeIndex const& index) const
    {
        auto [bins, labels] = get_time_bins(index);

        // If bins is empty, return an empty array
        if (bins.empty()) {
            return AssertResultIsOk(arrow::ChunkedArray::Make({}));
        }

        AssertFromStream(index.size() >= bins.size(), "up_sampling is not supported.");

        std::vector<int64_t> rep(bins.size());

        // Optimized exclusive scan to calculate differences between consecutive bins
        if (bins.size() > 100'000) {
            // Parallel version
            std::transform(std::execution::par, bins.begin() + 1, bins.end(), bins.begin(), rep.begin() + 1, std::minus<int64_t>());
        } else {
            // Sequential version
            std::transform(bins.begin() + 1, bins.end(), bins.begin(), rep.begin() + 1, std::minus<int64_t>());
        }

        // Set the first element manually to match NumPy behavior
        rep[0] = bins[0];

        // Compute total size and prepare vector for bulk append
        std::vector<int64_t> comp_ids;
        comp_ids.reserve(index.size());

        // Build the grouping indices that correspond to each value in the index
        for (int64_t i = 0; i < bins.size(); ++i)
        {
            int64_t count = rep[i];
            for (int64_t j = 0; j < count; ++j) {
                comp_ids.push_back(i);
            }
        }

        // Convert to Apache Arrow array and perform the take operation
        auto indices = labels->array().take(Array{factory::array::make_contiguous_array(comp_ids)}, false);
        return factory::array::make_array(indices.value());
    }

    std::pair<Scalar, Scalar> TimeGrouper::adjust_dates_anchored(DateTime const&    first_dt,
                                                                 DateTime const&    last_dt,
                                                                 OriginType const&  origin,
                                                                 TickHandler const& freq) const
    {

        auto first = first_dt.timestamp();
        auto last  = last_dt.timestamp();

        auto    freq_value = freq.nanos();
        int64_t origin_timestamp{};
        if (std::holds_alternative<EpochTimeGrouperOrigin>(origin))
        {
            switch (const auto origin_type = std::get<EpochTimeGrouperOrigin>(origin))
            {
                case EpochTimeGrouperOrigin::StartDay:
                    origin_timestamp = first_dt.normalize().timestamp().value;
                    break;
                case EpochTimeGrouperOrigin::Start:
                    origin_timestamp = first_dt.timestamp().value;
                    break;
                case EpochTimeGrouperOrigin::End:
                case EpochTimeGrouperOrigin::EndDay: {
                    auto origin_last    = origin_type == EpochTimeGrouperOrigin::End
                             ? last
                             : Scalar{last_dt}
                    .dt()
                    .ceil({arrow::compute::RoundTemporalOptions{
                        1, arrow::compute::CalendarUnit::DAY}})
                    .timestamp();
                    auto sub_freq_times = floor_div(origin_last.value - first.value, freq_value);

                    if (m_options.closed == EpochTimeGrouperClosedType::Left)
                    {
                        ++sub_freq_times;
                    }
                    origin_timestamp = origin_last.value - sub_freq_times * freq_value;
                    break;
                }
                default:
                    throw std::runtime_error("invalid origin type");
            }
        }
        else
        {
            origin_timestamp = std::get<DateTime>(origin).timestamp().value;
        }

        origin_timestamp += (m_options.offset ? m_options.offset->to_nanoseconds() : 0);

        auto first_tz = first_dt.tz;
        auto last_tz  = last_dt.tz;

        if (!first_tz.empty())
        {
            first = first_dt.tz_convert("UTC").timestamp();
        }
        if (!last_tz.empty())
        {
            last = last_dt.tz_convert("UTC").timestamp();
        }

        int64_t foffset     = pymod(first.value - origin_timestamp, freq_value);
        int64_t loffset     = pymod(last.value - origin_timestamp, freq_value);
        int64_t  fresult_int = 0;
        int64_t  lresult_int = 0;
        if (m_options.closed == EpochTimeGrouperClosedType::Right)
        {
            if (foffset > loffset)
            {
                fresult_int = first.value - foffset;
            }
            else
            {
                fresult_int = first.value - freq_value;
            }

            if (loffset > 0)
            {
                lresult_int = last.value + (freq_value - loffset);
            }
            else
            {
                lresult_int = last.value;
            }
        }
        else
        {
            if (foffset > 0)
            {
                fresult_int = first.value - foffset;
            }
            else
            {
                fresult_int = first.value;
            }

            if (loffset > 0)
            {
                lresult_int = last.value + (freq_value - loffset);
            }
            else
            {
                lresult_int = last.value + freq_value;
            }
        }

        Scalar fresult{arrow::TimestampScalar{fresult_int, first.type}};
        Scalar lresult{arrow::TimestampScalar{lresult_int, last.type}};

        if (!first_tz.empty())
        {
            fresult = fresult.dt().tz_localize("UTC").dt().tz_convert(first_tz);
        }
        if (!last_tz.empty())
        {
            lresult = lresult.dt().tz_localize(last_dt.tz).dt().tz_convert(last_tz);
        }

        return {fresult, lresult};
    }

    std::array<arrow::TimestampScalar, 2>
    TimeGrouper::get_timestamp_range_edges(DateTimeIndex const& index, DateTime const& first,
                                           DateTime const& last) const
    {
        arrow::TimestampScalar _first{index.dtype()};
        arrow::TimestampScalar _last{index.dtype()};
        const auto tick_handler = std::dynamic_pointer_cast<TickHandler>(m_options.freq);
        if (tick_handler)
        {
            auto     index_tz = first.tz;
            auto origin = m_options.origin;
            bool     origin_is_value = std::holds_alternative<DateTime>(m_options.origin);
            if (origin_is_value)
            {
                auto origin_value = std::get<DateTime>(m_options.origin);
                AssertFromStream(
                    origin_value.tz != index_tz,
                    "origin must have the same timezone as the index. origin: "
                        << origin_value.tz << "\tindex: " << index_tz);
            }
            else if (std::get<EpochTimeGrouperOrigin>(origin) ==
                     EpochTimeGrouperOrigin::Epoch)
            {
                AssertFromStream(index_tz.empty(),
                                          "index must have a timezone if origin is Epoch");
                origin = DateTime{
                    .date = {std::chrono::year{1970}, std::chrono::month{1}, std::chrono::day{1}},
                    .tz   = index_tz};
            }

            DateTime   first_dt = first;
            DateTime   last_dt  = last;
            const bool is_day_freq =
                std::dynamic_pointer_cast<DayHandler>(m_options.freq) != nullptr;
            if (is_day_freq)
            {
                first_dt = first_dt.tz_localize("");
                last_dt  = last_dt.tz_localize("");
                if (origin_is_value)
                {
                    origin = std::get<DateTime>(m_options.origin).tz_localize("");
                }
            }

            auto [first_adj, last_adj] =
                adjust_dates_anchored(first_dt, last_dt, origin, *tick_handler);

            if (is_day_freq)
            {
                first_adj = first_adj.dt().tz_localize(index_tz);
                last_adj  = last_adj.dt().tz_localize(index_tz, AmbiguousTimeHandling::RAISE,
                                                      NonexistentTimeHandling::SHIFT_FORWARD);
            }

            _first = first_adj.timestamp();
            _last  = last_adj.timestamp();
        }
        else
        {
            auto _first = first.normalize().timestamp();
            auto _last  = last.normalize().timestamp();

            if (m_options.closed == EpochTimeGrouperClosedType::Left)
            {
                _first = m_options.freq->rollforward(_first);
            }
            else
            {
                _first = m_options.freq->rsub(_first);
            }

            _last = m_options.freq->add(_last);
        }
        return {_first, _last};
    }

    std::pair<IndexPtr, Array> TimeGrouper::adjust_bin_edges(IndexPtr     binner,
                                                             Array const& ax_values) const
    {
        Array bin_edges;
        if (m_options.freq->is_end() || m_options.freq->type() == EpochOffsetType::Week)
        {
            if (m_options.closed == EpochTimeGrouperClosedType::Right)
            {
                auto edges_dt1 = binner->dt().tz_localize("");
                bin_edges      = edges_dt1.dt().tz_localize(ax_values.dt().tz());
            }
            else
            {
                bin_edges = binner->array();
            }

            if (bin_edges[-2] > ax_values.max())
            {
                bin_edges = bin_edges[{.stop = -1}];
                binner    = binner->iloc({.stop = -1});
            }
        }
        else
        {
            bin_edges = binner->array();
        }
        return {binner, bin_edges};
    }
} // namespace epoch_frame
