//
// Created by adesola on 1/20/25.
//

#pragma once
#include <arrow/api.h>
#include <span>
#include <ranges>
#include "common/asserts.h"
#include <set>


namespace epochframe::factory::array {
    arrow::ArrayPtr make_array(const arrow::ScalarVector &scalarVector,
                              std::shared_ptr<arrow::DataType> const &type);

    inline arrow::ChunkedArrayPtr make_array(const arrow::ArrayPtr &arrowPtr) {
        return AssertArrayResultIsOk(arrow::ChunkedArray::Make({arrowPtr}));
    }

    arrow::ChunkedArrayPtr make_random_array(int64_t length, int64_t seed=0);

    template<class T>
    arrow::ChunkedArrayPtr make_array(const auto &begin_, const auto &end_) {

        typename arrow::CTypeTraits<T>::BuilderType builder;
        AssertStatusIsOk(builder.Reserve(std::distance(begin_, end_)));

        if constexpr (std::is_floating_point_v<T>) {
            auto flagsView =
                    std::span<const T>(begin_, end_) | std::views::transform([](auto &&x) { return !std::isnan(x); });
            AssertStatusIsOk(builder.AppendValues(begin_, end_, flagsView.begin()));
        } else if constexpr (std::is_same_v<T, std::string>) {
            for (auto it = begin_; it != end_; ++it) {
                if constexpr (std::numeric_limits<T>::has_quiet_NaN) {
                    if (std::isnan(*it)) {
                        AssertStatusIsOk(builder.AppendNull());
                        continue;
                    }
                }
                AssertStatusIsOk(builder.Append(*it));
            }
        } else {
            AssertStatusIsOk(builder.AppendValues(begin_, end_));
        };
        return make_array(AssertResultIsOk(builder.Finish()));
    }

    template<class CType>
    arrow::ChunkedArrayPtr make_array(const std::vector<CType> &values) {
        return make_array<CType>(values.begin(), values.end());
    }

    template<class CType>
    arrow::ArrayPtr make_contiguous_array(const std::vector<CType> &values) {
        return make_array<CType>(values.begin(), values.end())->chunk(0);
    }

    template<class CType>
    arrow::ChunkedArrayPtr make_array(const std::span<const CType> &values) {
        return make_array<CType>(values.begin(), values.end());
    }

    arrow::ChunkedArrayPtr make_chunked_array(const arrow::ScalarVector &scalarVector);

    arrow::ArrayPtr make_array(const arrow::ChunkedArrayVector &arrowPtrList,
                              std::shared_ptr<arrow::DataType> const &type);

    arrow::ChunkedArrayPtr make_chunked_array(const arrow::Result<arrow::Datum> &datum);

    arrow::ArrayPtr make_contiguous_array(const arrow::Result<arrow::Datum> &datum);

    arrow::ChunkedArrayPtr make_chunked_array(const arrow::Datum &datum);

    arrow::ArrayPtr make_contiguous_array(const arrow::Datum &datum);
    arrow::ArrayPtr make_contiguous_array(const arrow::ChunkedArrayPtr &datum);

    arrow::ArrayPtr make_contiguous_array(const std::vector<Scalar> &set_array,
                                        std::shared_ptr<arrow::DataType> const &type);
    
    arrow::ArrayPtr make_timestamp_array(const std::vector<arrow::TimestampScalar> &set_array, arrow::TimeUnit::type unit=arrow::TimeUnit::NANO, std::string const &timezone="");


    arrow::Result<std::shared_ptr<arrow::Array>>
    array_to_struct_single_chunk(const std::vector<std::shared_ptr<arrow::Array>>& columns,
                                    const std::vector<std::shared_ptr<arrow::Field>>& fields);

}
