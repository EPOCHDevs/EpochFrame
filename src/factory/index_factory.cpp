// index_factory.cpp
#include "index_factory.h"
#include "index/range_index.h"
#include "index/datetime_index.h"
#include "index/object_index.h"
#include "common_utils/asserts.h"
#include <arrow/builder.h>
#include <arrow/array.h>
#include <arrow/type.h>



namespace epochframe::factory::index {

// 1. Range
     std::shared_ptr<arrow::UInt64Array>
    build_range_array(int64_t start, int64_t stop, int64_t step)
    {
        if (step == 0) {
            throw std::invalid_argument("RangeIndex step cannot be zero");
        }

        arrow::UInt64Builder builder;
        AssertStatusIsOk(builder.Reserve(static_cast<int64_t>(std::abs( (stop - start) / step) ) ) );

        if (step > 0) {
            for (int64_t val = start; val < stop; val += step) {
                builder.UnsafeAppend(static_cast<uint64_t>(val));
            }
        } else {
            // step < 0
            for (int64_t val = start; val > stop; val += step) {
                builder.UnsafeAppend(static_cast<uint64_t>(val));
            }
        }

        auto resultArr = AssertResultIsOk(builder.Finish());
        return std::static_pointer_cast<arrow::UInt64Array>(resultArr);
    }

    std::shared_ptr<Index> from_range(int64_t start, int64_t stop, int64_t step) {
        auto arr = build_range_array(start, stop, step);
        return std::make_shared<RangeIndex>(arr);
    }

    std::shared_ptr<Index> from_range(int64_t stop, int64_t step) {
        return from_range(0, stop, step);
    }

    std::shared_ptr<Index> from_range(int64_t stop) {
        return from_range(0, stop, 1);
    }

// 3. String index
    std::shared_ptr<Index> make_object_index(const std::vector<std::string> &data) {
        arrow::StringBuilder builder;
        AssertStatusIsOk(builder.Reserve(data.size()));

        for (auto &str: data) {
            AssertStatusIsOk(builder.Append(str));
        }

        return std::make_shared<ObjectIndex>(AssertCastResultIsOk<arrow::StringArray>(builder.Finish()));
    }

    std::shared_ptr<Index> make_object_index(const std::vector<arrow::ScalarPtr> &data) {
        arrow::StringBuilder builder;
        AssertStatusIsOk(builder.Reserve(data.size()));

        for (auto &str: data) {
            if (str && str->is_valid) {
                AssertStatusIsOk(builder.Append(str->ToString()));
            } else {
                builder.UnsafeAppendNull();
            }
        }

        return std::make_shared<ObjectIndex>(AssertCastResultIsOk<arrow::StringArray>(builder.Finish()));
    }

} // namespace epochframe::factory::index
