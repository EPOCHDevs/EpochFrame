// index_factory.cpp
#include "index_factory.h"
#include "index/range_index.h"
#include "index/datetime_index.h"
#include "index/object_index.h"
#include "index/struct_index.h"
#include "common/asserts.h"
#include <arrow/builder.h>
#include <arrow/array.h>
#include <arrow/type.h>
#include <arrow/compute/api.h>

namespace epochframe::factory::index {
    std::shared_ptr<Index> make_range(std::vector<uint64_t> const& x, MonotonicDirection monotonic_direction) {
        return std::make_shared<RangeIndex>(PtrCast<arrow::UInt64Array>(array::make_contiguous_array(x)), monotonic_direction);
    }
// 1. Range
    std::shared_ptr<arrow::UInt64Array>
    build_range_array(int64_t start, int64_t stop, int64_t step) {
        if (step == 0) {
            throw std::invalid_argument("RangeIndex step cannot be zero");
        }

        arrow::UInt64Builder builder;
        auto length = static_cast<int64_t>(std::abs((stop - start) / step)) + 1;
        AssertStatusIsOk(builder.Reserve(length));

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
                MonotonicDirection monotonic_direction = step > 0 ? MonotonicDirection::Increasing : MonotonicDirection::Decreasing;

        return std::make_shared<RangeIndex>(arr, monotonic_direction);
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

    IndexPtr make_index(arrow::ArrayPtr const& index_array, std::optional<MonotonicDirection> monotonic_direction, std::string const& name) {
        switch (index_array->type_id()) {
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
                return std::make_shared<RangeIndex>(AssertContiguousArrayResultIsOk(Cast(arrow::Datum{index_array}, option)), monotonic_direction, name);
            }
            case arrow::Type::STRING:
                case arrow::Type::STRING_VIEW:
            case arrow::Type::LARGE_STRING: {
                return std::make_shared<ObjectIndex>(index_array, name);
            }
            case arrow::Type::STRUCT: {
                return std::make_shared<StructIndex>(index_array, name);
            }
            default:
                break;
        }
        throw std::invalid_argument("Unknown index type: " + index_array->type()->ToString());
    }

    IndexPtr make_index(arrow::ChunkedArrayPtr const& index_array, std::optional<MonotonicDirection> monotonic_direction, std::string const& name) {
        return make_index(array::make_contiguous_array(index_array), monotonic_direction, name);
    }
} // namespace epochframe::factory::index
