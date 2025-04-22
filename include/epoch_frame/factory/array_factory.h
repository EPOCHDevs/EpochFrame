//
// Created by adesola on 1/20/25.
//

#pragma once
#include "common/asserts.h"
#include "epoch_frame/aliases.h"
#include "epoch_frame/array.h"
#include <arrow/api.h>
#include <cmath>
#include <epoch_frame/datetime.h>
#include <random>
#include <ranges>
#include <set>
#include <span>
namespace epoch_frame::factory::array
{
    arrow::ArrayPtr               make_array(const arrow::ScalarVector&              scalarVector,
                                             std::shared_ptr<arrow::DataType> const& type);
    arrow::ArrayPtr               make_array(const std::vector<Scalar>&              scalarVector,
                                             std::shared_ptr<arrow::DataType> const& type);
    inline arrow::ChunkedArrayPtr make_array(const arrow::ArrayPtr& arrowPtr)
    {
        AssertFromStream(arrowPtr, "arrowPtr is null");
        return AssertArrayResultIsOk(arrow::ChunkedArray::Make({arrowPtr}));
    }

    arrow::ChunkedArrayPtr make_random_array(int64_t length, int64_t seed = 0);

    arrow::ChunkedArrayPtr make_dt_array(const auto& begin_, const auto& end_)
    {
        if (begin_ == end_)
        {
            return std::make_shared<arrow::ChunkedArray>(std::vector<arrow::ArrayPtr>{},
                                                         arrow::timestamp(arrow::TimeUnit::NANO));
        }

        arrow::TimestampBuilder builder{arrow::timestamp(arrow::TimeUnit::NANO, begin_->tz()),
                                        arrow::default_memory_pool()};

        AssertStatusIsOk(builder.Reserve(std::distance(begin_, end_)));
        for (auto const& item : std::span{begin_, end_})
        {
            AssertStatusIsOk(builder.Append(item.timestamp().value));
        }
        return make_array(AssertResultIsOk(builder.Finish()));
    }

    template <class T> arrow::ChunkedArrayPtr make_array(const auto& begin_, const auto& end_)
    {
        if constexpr (std::same_as<T, DateTime>)
        {
            return make_dt_array(begin_, end_);
        }
        else
        {
            typename arrow::CTypeTraits<T>::BuilderType builder;
            AssertStatusIsOk(builder.Reserve(std::distance(begin_, end_)));

            if constexpr (std::is_floating_point_v<T>)
            {
                auto flagsView = std::span<const T>(begin_, end_) |
                                 std::views::transform([](auto&& x) { return !std::isnan(x); });
                AssertStatusIsOk(builder.AppendValues(begin_, end_, flagsView.begin()));
            }
            else if constexpr (std::is_same_v<T, std::string>)
            {
                for (auto it = begin_; it != end_; ++it)
                {
                    if constexpr (std::numeric_limits<T>::has_quiet_NaN)
                    {
                        if (std::isnan(*it))
                        {
                            AssertStatusIsOk(builder.AppendNull());
                            continue;
                        }
                    }
                    AssertStatusIsOk(builder.Append(*it));
                }
            }
            else
            {
                AssertStatusIsOk(builder.AppendValues(begin_, end_));
            };
            return make_array(AssertResultIsOk(builder.Finish()));
        }
    }

    template <class CType> arrow::ChunkedArrayPtr make_array(const std::vector<CType>& values)
    {
        return make_array<CType>(values.begin(), values.end());
    }

    template <class CType> arrow::ArrayPtr make_contiguous_array(const std::vector<CType>& values)
    {
        return make_array<CType>(values.begin(), values.end())->chunk(0);
    }

    template <class CType> arrow::ChunkedArrayPtr make_array(const std::span<const CType>& values)
    {
        return make_array<CType>(values.begin(), values.end());
    }

    arrow::ChunkedArrayPtr make_chunked_array(const arrow::ScalarVector&              scalarVector,
                                              std::shared_ptr<arrow::DataType> const& type);
    arrow::ChunkedArrayPtr make_chunked_array(const std::vector<Scalar>&              scalarVector,
                                              std::shared_ptr<arrow::DataType> const& type);

    inline arrow::ArrayPtr make_null_array(size_t                                  length,
                                           std::shared_ptr<arrow::DataType> const& type)
    {
        return AssertContiguousArrayResultIsOk(arrow::MakeArrayOfNull(type, length));
    }

    arrow::ArrayPtr make_array(const arrow::ChunkedArrayVector&        arrowPtrList,
                               std::shared_ptr<arrow::DataType> const& type);

    arrow::ChunkedArrayPtr make_chunked_array(const arrow::Result<arrow::Datum>& datum);

    arrow::ArrayPtr make_contiguous_array(const arrow::Result<arrow::Datum>& datum);

    arrow::ChunkedArrayPtr make_chunked_array(const arrow::Datum& datum);

    arrow::ArrayPtr make_contiguous_array(const arrow::Datum& datum);
    arrow::ArrayPtr make_contiguous_array(const arrow::ChunkedArrayPtr& datum);

    arrow::ArrayPtr make_contiguous_array(const std::vector<Scalar>&              set_array,
                                          std::shared_ptr<arrow::DataType> const& type);

    arrow::ArrayPtr make_timestamp_array(const std::vector<arrow::TimestampScalar>& set_array,
                                         arrow::TimeUnit::type unit     = arrow::TimeUnit::NANO,
                                         std::string const&    timezone = "");

    arrow::Result<std::shared_ptr<arrow::Array>>
    array_to_struct_single_chunk(const std::vector<std::shared_ptr<arrow::Array>>& columns,
                                 const std::vector<std::shared_ptr<arrow::Field>>& fields);

    arrow::ChunkedArrayPtr join_chunked_arrays(arrow::ArrayPtr const&        x,
                                               const arrow::ChunkedArrayPtr& arrays,
                                               bool                          join_right = true);

    /**
     * Creates a random array with normally distributed values
     * @param length Length of the array
     * @param seed Random number generator seed
     * @param mean Mean of the normal distribution
     * @param stddev Standard deviation of the normal distribution
     * @return ChunkedArray of double values
     */
    arrow::ChunkedArrayPtr make_random_normal_array(int64_t length, int64_t seed = 2,
                                                    double mean = 0.0, double stddev = 1.0);

    /**
     * Creates a random array with normally distributed values for a given date range
     * @param date_index DateTimeIndex to use
     * @param seed Random number generator seed
     * @param mean Mean of the normal distribution
     * @param stddev Standard deviation of the normal distribution
     * @return ChunkedArray of double values with the same length as the date_index
     */
    arrow::ChunkedArrayPtr
    make_random_normal_array_for_index(std::shared_ptr<arrow::ChunkedArray> date_index,
                                       int64_t seed = 2, double mean = 0.0, double stddev = 1.0);

    arrow::ChunkedArrayPtr make_random_normal_array_for_index(IndexPtr const& index,
                                                              int64_t seed = 2, double mean = 0.0,
                                                              double stddev = 1.0);
} // namespace epoch_frame::factory::array
