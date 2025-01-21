// index_factory.cpp
#include "index_factory.h"
#include "index/range_index.h"
#include "index/datetime_index.h"
#include "index/string_index.h"
#include "common_utils/asserts.h"
#include <arrow/builder.h>
#include <arrow/array.h>
#include <arrow/type.h>



namespace epochframe::factory::index {

// 1. Range
    std::shared_ptr<Index> range(int64_t start, int64_t stop, int64_t step) {
        // Build an Int64 array from [start..stop) with the given step
        arrow::UInt64Builder builder;
        int64_t dist = std::abs(stop - start);
        int64_t count = (dist > 0 && step > 0) ? (dist + step - 1) / step : 0;
        // Simple approach ignoring negative step etc. For brevity.

        AssertStatusIsOk(builder.Reserve(count));
        auto val = start;
        for (int64_t i = 0; i < count; ++i) {
            builder.UnsafeAppend(val);
            val += step;
        }

        // Now construct a RangeIndex from that array
        return std::make_shared<RangeIndex>(AssertCastResultIsOk<arrow::UInt64Array>(builder.Finish()));
    }

    std::shared_ptr<Index> from_range(int64_t stop, int64_t step) {
        return range(0, stop, step);
    }

// 2. Date range
    std::shared_ptr<Index> date_range(int64_t start_ns, int64_t end_ns, int64_t step_ns) {
        // We'll create a Timestamp array in Arrow (assuming NANO resolution)
        arrow::TimestampBuilder builder(arrow::timestamp(arrow::TimeUnit::NANO, "UTC"), arrow::default_memory_pool());

        if (step_ns <= 0) {
            step_ns = 1; // minimal fallback
        }

        int64_t dist = (end_ns - start_ns);
        int64_t count = (dist > 0) ? (dist / step_ns) : 0;

        AssertStatusIsOk(builder.Reserve(count));
        auto val = start_ns;
        for (int64_t i = 0; i < count; ++i) {
            builder.UnsafeAppend(val);
            val += step_ns;
        }
        std::shared_ptr<arrow::Array> arr;
        AssertStatusIsOk(builder.Finish(&arr));

        return std::make_shared<DateTimeIndex>(AssertCastResultIsOk<arrow::TimestampArray>(builder.Finish()));
    }

// 3. String index
    std::shared_ptr<Index> string_index(const std::vector<std::string> &data) {
        arrow::StringBuilder builder;
        AssertStatusIsOk(builder.Reserve(data.size()));

        for (auto &str: data) {
            builder.UnsafeAppend(str);
        }

        std::shared_ptr<arrow::Array> arr;
        AssertStatusIsOk(builder.Finish(&arr));

        return std::make_shared<StringIndex>(AssertCastResultIsOk<arrow::StringArray>(builder.Finish()));
    }

} // namespace epochframe::factory::index
