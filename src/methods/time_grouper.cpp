#include "time_grouper.h"
#include "common/asserts.h"
#include "common/python_utils.h"
#include "epoch_core/macros.h"
#include "epoch_frame/factory/index_factory.h"
#include "index/datetime_index.h"
#include <catch2/generators/catch_generators.hpp>
#include <execution>

namespace epoch_frame
{
    std::vector<int64_t> generate_bins(std::shared_ptr<arrow::TimestampArray> const& values,
                                       std::shared_ptr<arrow::TimestampArray> const& binner,
                                       epoch_core::GrouperClosedType                 closed)
    {
        const auto lenidx = values->length();
        const auto lenbin = binner->length();

        AssertFalseFromFormat(lenidx <= 0 || lenbin <= 0,
                              "Invalid length for values or for binner");
        AssertFalseFromFormat(values->Value(0) < binner->Value(0),
                              "Values falls before first bin. {} < {}.", values->Value(0),
                              binner->Value(0));
        AssertFalseFromFormat(values->Value(lenidx - 1) > binner->Value(lenbin - 1),
                              "Values falls after last bin. {} > {}.", values->Value(lenidx - 1),
                              binner->Value(lenbin - 1));

        std::vector<int64_t> bins(lenbin - 1);
        int64_t              j  = 0;
        int64_t              bc = 0;

        if (closed == epoch_core::GrouperClosedType::Right)
        {
            for (int64_t i = 0; i < lenbin - 1; ++i)
            {
                auto r_bin = binner->Value(i + 1);
                // count values in current bin, advance to next bin
                while (j < lenidx && values->Value(j) <= r_bin)
                    ++j;
                bins[bc] = j;
                ++bc;
            }
        }
        else
        {
            for (int64_t i = 0; i < lenbin - 1; ++i)
            {
                auto r_bin = binner->Value(i + 1);
                // count values in current bin, advance to next bin
                while (j < lenidx && values->Value(j) < r_bin)
                    ++j;
                bins[bc] = j;
                ++bc;
            }
        }
        return bins;
    }

    std::vector<int64_t> generate_bins(Array const& values, Array const& binner,
                                       epoch_core::GrouperClosedType closed)
    {
        AssertFromFormat(values.null_count() == 0, "Values cannot contain null values");
        AssertFromFormat(binner.null_count() == 0, "Bin edges cannot contain null values");
        return generate_bins(values.to_timestamp_view(), binner.to_timestamp_view(), closed);
    }

    TimeGrouper::TimeGrouper(const TimeGrouperOptions& options) : m_options(options)
    {
        AssertFromStream(m_options.freq, "Frequency must be set");

        bool origin_is_value = std::holds_alternative<DateTime>(m_options.origin);

        if (m_options.freq->is_end() || m_options.freq->type() == epoch_core::EpochOffsetType::Week)
        {
            if (m_options.closed == epoch_core::GrouperClosedType::Null)
            {
                m_options.closed = epoch_core::GrouperClosedType::Right;
            }
            if (m_options.label == epoch_core::GrouperLabelType::Null)
            {
                m_options.label = epoch_core::GrouperLabelType::Right;
            }
        }
        else
        {
            if (!origin_is_value)
            {
                auto origin = std::get<epoch_core::GrouperOrigin>(m_options.origin);
                if (origin == epoch_core::GrouperOrigin::End ||
                    origin == epoch_core::GrouperOrigin::EndDay)
                {
                    if (m_options.closed == epoch_core::GrouperClosedType::Null)
                    {
                        m_options.closed = epoch_core::GrouperClosedType::Right;
                    }
                    if (m_options.label == epoch_core::GrouperLabelType::Null)
                    {
                        m_options.label = epoch_core::GrouperLabelType::Right;
                    }
                }
            }
            else
            {
                if (m_options.closed == epoch_core::GrouperClosedType::Null)
                {
                    m_options.closed = epoch_core::GrouperClosedType::Left;
                }
                if (m_options.label == epoch_core::GrouperLabelType::Null)
                {
                    m_options.label = epoch_core::GrouperLabelType::Left;
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

        if (m_options.closed == epoch_core::GrouperClosedType::Right)
        {
            labels = binner;
            if (m_options.label == epoch_core::GrouperLabelType::Right)
            {
                labels = labels->iloc({1});
            }
        }
        else if (m_options.label == epoch_core::GrouperLabelType::Right)
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
        if (bins.empty())
        {
            return AssertResultIsOk(arrow::ChunkedArray::Make({}));
        }

        // AssertFromStream(index.size() >= bins.size(), "up_sampling is not supported.");

        std::vector<int64_t> rep(bins.size());

        // Optimized exclusive scan to calculate differences between consecutive bins
        if (bins.size() > 100'000)
        {
            // Parallel version
            std::transform(std::execution::par, bins.begin() + 1, bins.end(), bins.begin(),
                           rep.begin() + 1, std::minus<int64_t>());
        }
        else
        {
            // Sequential version
            std::transform(bins.begin() + 1, bins.end(), bins.begin(), rep.begin() + 1,
                           std::minus<int64_t>());
        }

        // Set the first element manually to match NumPy behavior
        rep[0] = bins[0];

        // Compute total size and prepare vector for bulk append
        std::vector<int64_t> comp_ids;
        comp_ids.reserve(index.size());

        // Build the grouping indices that correspond to each value in the index
        for (size_t i = 0; i < bins.size(); ++i)
        {
            int64_t count = rep[i];
            for (int64_t j = 0; j < count; ++j)
            {
                comp_ids.push_back(i);
            }
        }

        // Convert to Apache Arrow array and perform the take operation
        auto indices =
            labels->array().take(Array{factory::array::make_contiguous_array(comp_ids)}, false);
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
        if (std::holds_alternative<epoch_core::GrouperOrigin>(origin))
        {
            switch (const auto origin_type = std::get<epoch_core::GrouperOrigin>(origin))
            {
                case epoch_core::GrouperOrigin::StartDay:
                    origin_timestamp = first_dt.normalize().timestamp().value;
                    break;
                case epoch_core::GrouperOrigin::Start:
                    origin_timestamp = first_dt.timestamp().value;
                    break;
                case epoch_core::GrouperOrigin::End:
                case epoch_core::GrouperOrigin::EndDay:
                {
                    auto origin_last    = origin_type == epoch_core::GrouperOrigin::End
                                              ? last
                                              : Scalar{last_dt}
                                                 .dt()
                                                 .ceil({arrow::compute::RoundTemporalOptions{
                                                     1, arrow::compute::CalendarUnit::DAY}})
                                                 .timestamp();
                    auto sub_freq_times = floor_div(origin_last.value - first.value, freq_value);

                    if (m_options.closed == epoch_core::GrouperClosedType::Left)
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

        auto first_tz = first_dt.tz();
        auto last_tz  = last_dt.tz();

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
        int64_t fresult_int = 0;
        int64_t lresult_int = 0;
        if (m_options.closed == epoch_core::GrouperClosedType::Right)
        {
            if (foffset > 0)
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
            lresult = lresult.dt().tz_localize("UTC").dt().tz_convert(last_tz);
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
            auto index_tz        = first.tz();
            auto origin          = m_options.origin;
            bool origin_is_value = std::holds_alternative<DateTime>(m_options.origin);
            if (origin_is_value)
            {
                auto origin_value = std::get<DateTime>(m_options.origin);
                AssertFalseFromStream((origin_value.tz() == "") != (index_tz == ""),
                                      "origin must have the same timezone as the index. origin: "
                                          << origin_value.tz() << "\tindex: " << index_tz);
            }
            else if (std::get<epoch_core::GrouperOrigin>(origin) ==
                     epoch_core::GrouperOrigin::Epoch)
            {
                AssertFromStream(index_tz.empty(), "index must have a timezone if origin is Epoch");
                origin = DateTime{
                    Date{std::chrono::year{1970}, std::chrono::month{1}, std::chrono::day{1}},
                    Time{.tz = index_tz}};
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
            _first = first.normalize().timestamp();
            _last  = last.normalize().timestamp();

            if (m_options.closed == epoch_core::GrouperClosedType::Left)
            {
                _first = m_options.freq->rollback(_first);
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
        Array bin_edges = binner->array();
        if (m_options.freq->is_end() || m_options.freq->type() == epoch_core::EpochOffsetType::Week)
        {
            if (m_options.closed == epoch_core::GrouperClosedType::Right)
            {
                bin_edges = bin_edges.dt().tz_localize("") + Scalar{TimeDelta{chrono_days{1}}} -
                            Scalar{TimeDelta{chrono_microseconds{1}}};
                bin_edges = bin_edges.dt().tz_localize(arrow_utils::get_tz(binner->dtype()));
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
