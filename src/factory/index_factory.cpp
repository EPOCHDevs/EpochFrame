// index_factory.cpp
#include "epoch_frame/factory/index_factory.h"
#include "common/asserts.h"
#include "index/datetime_index.h"
#include "index/object_index.h"
#include "index/range_index.h"
#include "index/struct_index.h"
#include <arrow/array.h>
#include <arrow/builder.h>
#include <arrow/compute/api.h>
#include <arrow/type.h>

namespace epoch_frame::factory::index
{
    IndexPtr make_range(std::vector<uint64_t> const& x, MonotonicDirection monotonic_direction)
    {
        return std::make_shared<RangeIndex>(
            PtrCast<arrow::UInt64Array>(array::make_contiguous_array(x)), monotonic_direction);
    }
    // 1. Range
    std::shared_ptr<arrow::UInt64Array> build_range_array(int64_t start, int64_t stop, int64_t step)
    {
        if (step == 0)
        {
            throw std::invalid_argument("RangeIndex step cannot be zero");
        }

        const bool forward = step > 0;
        if ((forward && start >= stop) || (!forward && start <= stop))
        {
            // Empty range
            arrow::UInt64Builder builder;
            AssertStatusIsOk(builder.Reserve(0));
            return std::static_pointer_cast<arrow::UInt64Array>(
                AssertContiguousArrayResultIsOk(builder.Finish()));
        }

        const __int128 diff     = static_cast<__int128>(stop) - static_cast<__int128>(start);
        const __int128 step_abs = static_cast<__int128>(step > 0 ? step : -step);

        // Number of terms for exclusive stop: ceil(|diff|/|step|) but without overflow
        __int128 len128 = (((diff > 0 ? diff : -diff) - 1) / step_abs) + 1;
        if (len128 < 0)
            len128 = 0;
        const int64_t length = static_cast<int64_t>(
            std::min<__int128>(len128, static_cast<__int128>(std::numeric_limits<int64_t>::max())));

        arrow::UInt64Builder builder;
        AssertStatusIsOk(builder.Reserve(length));

        if (forward)
        {
            for (int64_t v = start; v < stop; v += step)
            {
                builder.UnsafeAppend(static_cast<uint64_t>(v));
            }
        }
        else
        {
            for (int64_t v = start; v > stop; v += step)
            {
                builder.UnsafeAppend(static_cast<uint64_t>(v));
            }
        }
        auto resultArr = AssertResultIsOk(builder.Finish());
        return std::static_pointer_cast<arrow::UInt64Array>(resultArr);
    }

    IndexPtr from_range(int64_t start, int64_t stop, int64_t step)
    {
        auto               arr = build_range_array(start, stop, step);
        MonotonicDirection monotonic_direction =
            step > 0 ? MonotonicDirection::Increasing : MonotonicDirection::Decreasing;

        return std::make_shared<RangeIndex>(arr, monotonic_direction);
    }

    IndexPtr from_range(int64_t stop)
    {
        return from_range(0, stop, 1);
    }

    // 3. String index
    IndexPtr make_object_index(const std::vector<std::string>& data)
    {
        arrow::StringBuilder builder;
        AssertStatusIsOk(builder.Reserve(data.size()));

        for (auto& str : data)
        {
            AssertStatusIsOk(builder.Append(str));
        }

        return std::make_shared<ObjectIndex>(
            AssertCastResultIsOk<arrow::StringArray>(builder.Finish()));
    }

    IndexPtr make_object_index(const std::vector<arrow::ScalarPtr>& data)
    {
        arrow::StringBuilder builder;
        AssertStatusIsOk(builder.Reserve(data.size()));

        for (auto& str : data)
        {
            if (str && str->is_valid)
            {
                AssertStatusIsOk(builder.Append(str->ToString()));
            }
            else
            {
                builder.UnsafeAppendNull();
            }
        }

        return std::make_shared<ObjectIndex>(
            AssertCastResultIsOk<arrow::StringArray>(builder.Finish()));
    }

    IndexPtr make_index(arrow::ArrayPtr const&            index_array,
                        std::optional<MonotonicDirection> monotonic_direction,
                        std::string const&                name)
    {
        switch (index_array->type_id())
        {
            case arrow::Type::UINT64:
                return std::make_shared<RangeIndex>(index_array, monotonic_direction, name);
            case arrow::Type::FLOAT:
            case arrow::Type::HALF_FLOAT:
            case arrow::Type::DOUBLE:
            case arrow::Type::UINT32:
            case arrow::Type::UINT16:
            case arrow::Type::UINT8:
            case arrow::Type::INT64:
            case arrow::Type::INT32:
            case arrow::Type::INT16:
            case arrow::Type::INT8:
            {
                arrow::compute::CastOptions option{true};
                option.to_type = arrow::TypeHolder{arrow::uint64()};
                return std::make_shared<RangeIndex>(
                    AssertContiguousArrayResultIsOk(Cast(arrow::Datum{index_array}, option)),
                    monotonic_direction, name);
            }
            case arrow::Type::STRING:
            case arrow::Type::STRING_VIEW:
            case arrow::Type::LARGE_STRING:
            {
                return std::make_shared<ObjectIndex>(index_array, name);
            }
            case arrow::Type::STRUCT:
            {
                return std::make_shared<StructIndex>(index_array, name);
            }
            case arrow::Type::TIMESTAMP:
            {
                auto type = std::static_pointer_cast<arrow::TimestampType>(index_array->type());

                if (type->unit() != arrow::TimeUnit::NANO)
                {
                    return std::make_shared<DateTimeIndex>(
                        Array{index_array}
                            .cast(arrow::timestamp(arrow::TimeUnit::NANO, type->timezone()))
                            .value(),
                        name);
                }
                return std::make_shared<DateTimeIndex>(index_array, name);

                break;
            }
            default:
                break;
        }

        if (arrow::is_temporal(index_array->type_id()))
        {
            return std::make_shared<DateTimeIndex>(
                Array{index_array}.cast(arrow::timestamp(arrow::TimeUnit::NANO, "")).value(), name);
        }

        throw std::invalid_argument("Unknown index type: " + index_array->type()->ToString());
    }

    IndexPtr make_index(arrow::ChunkedArrayPtr const&     index_array,
                        std::optional<MonotonicDirection> monotonic_direction,
                        std::string const&                name)
    {
        return make_index(array::make_contiguous_array(index_array), monotonic_direction, name);
    }

    IndexPtr make_empty_index(arrow::DataTypePtr const&         type,
                              std::optional<MonotonicDirection> monotonic_direction,
                              std::string const&                name)
    {
        return make_index(array::make_null_array(0, type), monotonic_direction, name);
    }

    std::shared_ptr<DateTimeIndex> date_range(std::vector<int64_t> const& arr,
                                              arrow::DataTypePtr const&   type)
    {
        arrow::TimestampBuilder builder(type, arrow::default_memory_pool());
        AssertStatusIsOk(builder.AppendValues(arr));
        return std::make_shared<DateTimeIndex>(AssertContiguousArrayResultIsOk(builder.Finish()),
                                               "");
    }

    std::shared_ptr<DateTimeIndex> date_range_internal(arrow::TimestampScalar t, uint32_t period,
                                                       DateOffsetHandlerPtr const& offset)
    {
        std::vector<int64_t> result;
        result.reserve(period);

        if (period == 0)
        {
            return std::make_shared<DateTimeIndex>(arrow::MakeEmptyArray(t.type).MoveValueUnsafe());
        }

        while (period > 0)
        {
            result.push_back(t.value);
            t = offset->add(t);
            --period;
        }
        return date_range(result, t.type);
    }

    std::shared_ptr<DateTimeIndex> date_range_internal(arrow::TimestampScalar        start,
                                                       arrow::TimestampScalar const& end,
                                                       DateOffsetHandlerPtr const&   offset)
    {
        std::vector<int64_t> result;

        // Safe, saturating estimate for reserve (optional but helpful)
        const __int128 ns_per_day = static_cast<__int128>(86'400'000'000'000LL);
        __int128       diff = static_cast<__int128>(end.value) - static_cast<__int128>(start.value);
        int64_t        days = 0;
        if (diff > 0)
        {
            days = static_cast<int64_t>(std::min<__int128>(
                diff / ns_per_day, static_cast<__int128>(std::numeric_limits<int64_t>::max())));
        }
        constexpr int64_t kCap = 50'000'000; // hard cap to avoid absurd reserves
        if (days > 0)
        {
            result.reserve(static_cast<size_t>(std::min<int64_t>(days + 1, kCap)));
        }

        while (start.value <= end.value)
        {
            result.push_back(start.value);
            const auto next = offset->add(start);
            AssertFromStream(next.value > start.value,
                             "offset " << offset->name() << " did not increment date");
            start = next;
        }
        return date_range(result, start.type);
    }

    std::string infer_tz_from_endpoints(std::optional<arrow::TimestampScalar> const& start,
                                        std::optional<arrow::TimestampScalar> const& end,
                                        std::string const&                           tz)
    {
        auto startType =
            start ? std::dynamic_pointer_cast<arrow::TimestampType>(start->type) : nullptr;
        auto endType = end ? std::dynamic_pointer_cast<arrow::TimestampType>(end->type) : nullptr;

        if (startType == nullptr && endType == nullptr)
        {
            throw std::runtime_error("start and end cannot both be null");
        }

        std::string inferred_tz;
        if (startType && endType)
        {
            if (startType->Equals(endType))
            {
                inferred_tz = startType->timezone();
            }
            else
            {
                throw std::runtime_error(std::format("start and end must have same type. {} != {}",
                                                     startType->timezone(), endType->timezone()));
            }
        }
        else if (startType)
        {
            inferred_tz = startType->timezone();
        }
        else
        {
            inferred_tz = endType->timezone();
        }

        if (!tz.empty() && !inferred_tz.empty())
        {
            AssertFromStream(tz == inferred_tz,
                             "Inferred time zone not equal to passed time zone. tz="
                                 << tz << ", inferred_tz=" << inferred_tz);
        }
        else if (!inferred_tz.empty())
        {
            return inferred_tz;
        }
        return tz;
    }

    std::optional<arrow::TimestampScalar>
    maybe_localize_point(std::optional<arrow::TimestampScalar> const& ts,
                         DateOffsetHandlerPtr const& freq, std::string const& tz,
                         AmbiguousTimeHandling ambigous, NonexistentTimeHandling nonexistent)
    {
        if (ts && arrow_utils::get_tz(*ts).empty())
        {
            return Scalar{*ts}
                .dt()
                .tz_localize(std::dynamic_pointer_cast<TickHandler>(freq) ? tz : "", ambigous,
                             nonexistent)
                .timestamp();
        }
        return ts;
    }

    IndexPtr date_range(DateRangeOptions const& options)
    {
        AssertFromFormat(options.offset, "date_range requires a freq");
        AssertFromFormat(options.periods || options.end.has_value(),
                         "date_range requires send or start && period");

        auto                   tz = infer_tz_from_endpoints(options.start, options.end, options.tz);
        arrow::TimestampScalar start              = options.start;
        std::optional<arrow::TimestampScalar> end = options.end;
        if (!tz.empty())
        {
            start = maybe_localize_point(start, options.offset, options.tz, options.ambiguous,
                                         options.nonexistent)
                        .value();
            end = maybe_localize_point(end, options.offset, options.tz, options.ambiguous,
                                       options.nonexistent);
        }

        if (std::dynamic_pointer_cast<DayHandler>(options.offset))
        {
            start = Scalar{start}.dt().tz_localize("").timestamp();
            if (end)
            {
                end = Scalar{*end}.dt().tz_localize("").timestamp();
            }
        }

        start = options.offset->rollforward(start);
        std::shared_ptr<DateTimeIndex> index;
        if (end)
        {
            end   = options.offset->rollback(*end);
            index = date_range_internal(start, *end, options.offset);
        }
        else
        {
            index = date_range_internal(start, *options.periods, options.offset);
        }

        auto endpoint_tz = Scalar{options.start}.dt().tz();
        if (!tz.empty() && endpoint_tz.empty())
        {
            return index->Make(
                index->dt().tz_localize(tz, options.ambiguous, options.nonexistent).value());
        }
        return index->replace_tz(tz);
    }

    IndexPtr make_datetime_index(std::vector<int64_t> const& timestamps, std::string const& name,
                                 std::string const& tz)
    {
        arrow::TimestampBuilder builder(arrow::timestamp(arrow::TimeUnit::NANO, tz),
                                        arrow::default_memory_pool());
        if (std::ranges::is_sorted(timestamps))
        {
            AssertStatusIsOk(builder.AppendValues(timestamps));
        }
        else
        {
            auto sorted = timestamps;
            std::ranges::sort(sorted);
            AssertStatusIsOk(builder.AppendValues(sorted));
        }

        return std::make_shared<DateTimeIndex>(AssertContiguousArrayResultIsOk(builder.Finish()),
                                               name);
    }

    IndexPtr make_datetime_index(std::vector<arrow::TimestampScalar> const& timestamps,
                                 std::string const& name, std::string const& tz)
    {
        std::vector<int64_t> scalars;
        scalars.reserve(timestamps.size());
        for (auto const& timestamp : timestamps)
        {
            scalars.emplace_back(timestamp.value);
        }
        return make_datetime_index(scalars, name, tz);
    }

    IndexPtr make_datetime_index(std::vector<DateTime> const& timestamps, std::string const& name,
                                 std::string const& tz)
    {
        std::vector<int64_t>            scalars;
        std::unordered_set<std::string> timezones;
        scalars.reserve(timestamps.size());
        for (auto const& timestamp : timestamps)
        {
            scalars.emplace_back(timestamp.timestamp().value);
            timezones.emplace(timestamp.tz());
        }
        if (timezones.size() == 2 && timezones.contains("UTC") && timezones.contains(""))
        {
            AssertFromStream(timezones.contains(tz), "All timestamps must have the same timezone");
            return make_datetime_index(scalars, name, tz);
        }

        AssertFromStream(timezones.size() <= 1, "All timestamps must have the same timezone");
        return make_datetime_index(scalars, name,
                                   (tz.empty() && !timezones.empty()) ? *timezones.begin() : tz);
    }
} // namespace epoch_frame::factory::index
