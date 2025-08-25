//
// Created by adesola on 1/20/25.
//

#include "arrow_compute_utils.h"
#include "common/epoch_thread_pool.h"
#include "epoch_frame/factory/array_factory.h"
#include "epoch_frame/frame_or_series.h"
#include "index/arrow_index.h"
#include <arrow/api.h>
#include <epoch_core/macros.h>
#include <fmt/format.h>
#include <memory>
#include <oneapi/tbb/partitioner.h>
#include <stdexcept>
#include <tbb/parallel_for.h>
#include <vector>

namespace epoch_frame::arrow_utils
{
    IndexPtr integer_slice_index(const IIndex& index, size_t start, size_t length)
    {
        return index.Make(slice_array(index.array().value(), start, length));
    }

    IndexPtr integer_slice_index(const IIndex& index, size_t start, size_t length, int64_t step)
    {
        return index.Make(slice_array(index.array().value(), start, length, step));
    }

    arrow::ScalarPtr call_unary_agg_compute(const arrow::Datum&                    input,
                                            const std::string&                     function_name,
                                            arrow::compute::FunctionOptions const& options)
    {
        const auto datum = call_unary_compute(input, function_name, &options);

        if (datum.is_scalar())
        {
            return datum.scalar();
        }
        if (datum.is_array())
        {
            const auto arr = datum.make_array();
            AssertFromFormat(arr->length() == 1, "Failed to create Scalar from agg result array");
            return AssertResultIsOk(arr->GetScalar(0));
        }
        return MakeNullScalar(arrow::null());
    }

    arrow::TablePtr apply_function_to_table(
        const arrow::TablePtr&                                               table,
        std::function<arrow::Datum(arrow::Datum const&, std::string const&)> func,
        bool                                                                 merge_chunks)
    {
        std::vector<std::shared_ptr<arrow::ChunkedArray>> processed_columns(table->num_columns());
        arrow::FieldVector                                fields = table->schema()->fields();

        EpochThreadPool::getInstance().execute(
            [&]
            {
                tbb::parallel_for(
                    tbb::blocked_range<int64_t>(0, table->num_columns()),
                    [&](const tbb::blocked_range<int64_t>& r)
                    {
                        for (int64_t i = r.begin(); i != r.end(); ++i)
                        {
                            auto merged = merge_chunks
                                              ? arrow::Datum(AssertContiguousArrayResultIsOk(
                                                    arrow::Concatenate(table->column(i)->chunks())))
                                              : arrow::Datum(table->column(i));
                            auto result = func(merged, fields[i]->name());
                            if (result.kind() == arrow::Datum::ARRAY)
                            {
                                processed_columns[i] =
                                    std::make_shared<arrow::ChunkedArray>(result.make_array());
                                fields[i] = field(fields[i]->name(), processed_columns[i]->type());
                            }
                            else if (result.kind() == arrow::Datum::CHUNKED_ARRAY)
                            {
                                processed_columns[i] = result.chunked_array();
                                fields[i] = field(fields[i]->name(), processed_columns[i]->type());
                            }
                            else
                            {
                                throw std::runtime_error(
                                    std::format("Invalid arrow::Datum kind: {}",
                                                arrow::ToString(result.kind())));
                            }
                        }
                    },
                    tbb::simple_partitioner());
            });

        return arrow::Table::Make(arrow::schema(fields), processed_columns);
    }

    // Helper to get a scalar from an array at a specific index
    std::shared_ptr<arrow::Scalar> GetScalar(const std::shared_ptr<arrow::Array>& array,
                                             int64_t                              index)
    {
        if (!array || index < 0 || index >= array->length())
        {
            return nullptr;
        }

        auto result = array->GetScalar(index);
        if (!result.ok())
        {
            throw std::runtime_error("Failed to get scalar: " + result.status().ToString());
        }

        return result.ValueOrDie();
    }

    chrono_time_point get_time_point(const Scalar& scalar)
    {
        return get_time_point(scalar.timestamp());
    }

    arrow::ArrayPtr map(const arrow::ArrayPtr&                      array,
                        const std::function<Scalar(const Scalar&)>& func, bool ignore_nulls)
    {
        if (!array)
        {
            return nullptr;
        }
        AssertFromStream(func, "Function is not callable");

        std::vector<arrow::ScalarPtr> scalars;
        scalars.reserve(array->length());
        const auto null_scalar = arrow::MakeNullScalar(array->type());

        for (int64_t i = 0; i < array->length(); ++i)
        {
            auto scalar = AssertResultIsOk(array->GetScalar(i));
            if (!scalar->is_valid && ignore_nulls)
            {
                scalars.push_back(null_scalar);
                continue;
            }

            scalars.push_back(func(Scalar(scalar)).value());
        }

        arrow::DataTypePtr data_type;
        const auto         iter = std::ranges::find_if(scalars, [&](const arrow::ScalarPtr& scalar)
                                                       { return scalar->is_valid; });
        if (iter == scalars.end())
        {
            data_type = array->type();
        }
        else
        {
            data_type = (*iter)->type;
        }

        auto builder_result = arrow::MakeBuilder(data_type);
        AssertFromFormat(builder_result.ok(), "Failed to create builder for array type");
        auto builder = builder_result.MoveValueUnsafe();

        auto x = builder->AppendScalars(scalars);
        if (x.ok())
        {
            return AssertResultIsOk(builder->Finish());
        }
        std::stringstream ss;
        ss << x.message() << "\nValid Scalar Builder:\n";
        for (const auto& scalar : scalars)
        {
            ss << scalar->ToString() << "\n";
        }
        throw std::runtime_error(ss.str());
    }

    arrow::ChunkedArrayPtr map(const arrow::ChunkedArrayPtr&               array,
                               const std::function<Scalar(const Scalar&)>& func, bool ignore_nulls)
    {
        if (!array)
        {
            return nullptr;
        }

        std::vector<std::shared_ptr<arrow::Array>> chunks;

        // Process each chunk in the column
        for (int chunk_idx = 0; chunk_idx < array->num_chunks(); ++chunk_idx)
        {
            auto chunk        = array->chunk(chunk_idx);
            auto mapped_chunk = map(chunk, func, ignore_nulls);
            chunks.push_back(mapped_chunk);
        }

        // Create a chunked array from the mapped chunks
        return std::make_shared<arrow::ChunkedArray>(chunks);
    }

    arrow::TablePtr map(const arrow::TablePtr&                      table,
                        const std::function<Scalar(const Scalar&)>& func, bool ignore_nulls)
    {
        if (!table)
        {
            return nullptr;
        }

        // Create a vector to store the result columns
        std::vector<std::shared_ptr<arrow::ChunkedArray>> result_columns;
        result_columns.reserve(table->num_columns());

        // Process each column
        for (int col = 0; col < table->num_columns(); ++col)
        {
            result_columns.push_back(map(table->column(col), func, ignore_nulls));
        }

        // Create a new table with the original schema and the mapped columns
        return arrow::Table::Make(table->schema(), result_columns);
    }

    chrono_year_month_day get_year_month_day(const arrow::TimestampScalar& scalar)
    {
        auto ymd =
            AssertCastScalarResultIsOk<arrow::StructScalar>(arrow::compute::YearMonthDay(scalar));
        return {
            chrono_year(AssertCastScalarResultIsOk<arrow::Int64Scalar>(ymd.field("year")).value),
            chrono_month(AssertCastScalarResultIsOk<arrow::Int64Scalar>(ymd.field("month")).value),
            chrono_day(AssertCastScalarResultIsOk<arrow::Int64Scalar>(ymd.field("day")).value),
        };
    }

    chrono_year get_year(const arrow::TimestampScalar& scalar)
    {
        auto year = AssertCastScalarResultIsOk<arrow::Int64Scalar>(arrow::compute::Year(scalar));
        return chrono_year(year.value);
    }

    chrono_month get_month(const arrow::TimestampScalar& scalar)
    {
        auto month = AssertCastScalarResultIsOk<arrow::Int64Scalar>(arrow::compute::Month(scalar));
        return chrono_month(month.value);
    }

    chrono_day get_day(const arrow::TimestampScalar& scalar)
    {
        auto day = AssertCastScalarResultIsOk<arrow::Int64Scalar>(arrow::compute::Day(scalar));
        return chrono_day(day.value);
    }

    std::chrono::nanoseconds duration(const arrow::TimestampScalar& scalar1,
                                      const arrow::TimestampScalar& scalar2)
    {
        AssertFromFormat(scalar1.type->Equals(scalar2.type),
                         "duration between incompatible timestamp.");
        auto type = std::dynamic_pointer_cast<arrow::TimestampType>(scalar1.type);
        AssertFromFormat(type && type->unit() == arrow::TimeUnit::NANO,
                         "duration only supports nanoseconds.");

        auto diff = scalar1.value - scalar2.value;
        auto n    = std::chrono::nanoseconds(diff);
        return n;
    }

    std::string get_tz(const arrow::DataTypePtr& type)
    {
        AssertFromStream(type, "Type is not valid");

        auto dt = std::dynamic_pointer_cast<arrow::TimestampType>(type);
        if (!dt)
        {
            return ""; // Return empty string for non-timestamp types instead of asserting
        }

        std::string tz = dt->timezone();

        // Handle empty timezone (naive timestamps)
        if (tz.empty())
        {
            return "";
        }

        // Normalize timezone format if needed
        // Some timezone formats may need standardization
        // E.g., "UTC+01:00" vs "+01:00" vs "Etc/GMT-1"

        return tz;
    }

    TableOrArray call_unary_compute_table_or_array(const TableOrArray& table,
                                                   std::string const&  function_name,
                                                   arrow::compute::FunctionOptions const* options)
    {
        if (table.is_table())
        {
            return TableOrArray{apply_function_to_table(
                table.table(), [&](const arrow::Datum& arr, std::string const&)
                { return call_unary_compute_contiguous_array(arr, function_name, options); })};
        }
        return TableOrArray{call_unary_compute_array(table.datum(), function_name, options)};
    }

    TableOrArray call_compute_is_in(const TableOrArray& table, const arrow::ArrayPtr& values)
    {
        arrow::compute::SetLookupOptions options{
            values, arrow::compute::SetLookupOptions::NullMatchingBehavior::MATCH};
        return call_unary_compute_table_or_array(table, "is_in", &options);
    }

    TableOrArray call_compute_index_in(const TableOrArray& table, const arrow::ArrayPtr& values)
    {
        arrow::compute::SetLookupOptions options{
            values, arrow::compute::SetLookupOptions::NullMatchingBehavior::MATCH};
        return call_unary_compute_table_or_array(table, "index_in", &options);
    }

    TableOrArray diff(const TableOrArray& table, int64_t periods, bool pad)
    {
        arrow::compute::PairwiseOptions options{periods};

        // Process the array chunk by chunk to avoid "Vector kernel cannot execute chunkwise" error
        if (table.is_chunked_array())
        {
            auto                                       chunked_array = table.chunked_array();
            std::vector<std::shared_ptr<arrow::Array>> result_chunks;

            for (int i = 0; i < chunked_array->num_chunks(); i++)
            {
                auto chunk = chunked_array->chunk(i);
                auto array_result =
                    arrow::compute::CallFunction("pairwise_diff", {chunk}, &options);
                if (!array_result.ok())
                {
                    throw std::runtime_error("Error calculating diff: " +
                                             array_result.status().ToString());
                }
                result_chunks.push_back(array_result.ValueOrDie().make_array());
            }

            auto result = std::make_shared<arrow::ChunkedArray>(result_chunks);
            if (pad)
            {
                return TableOrArray{result};
            }
            else
            {
                auto abs_periods = std::abs(periods);
                auto nans        = factory::array::make_null_array(abs_periods, result->type());
                bool merge_right = periods < 0;
                return TableOrArray{factory::array::join_chunked_arrays(nans, result, merge_right)};
            }
        }
        else
        {
            // For tables, process each column
            return TableOrArray{apply_function_to_table(
                table.table(),
                [&](const arrow::Datum& arr, std::string const&)
                {
                    const auto&                                chunked_array = arr.chunked_array();
                    std::vector<std::shared_ptr<arrow::Array>> result_chunks;

                    for (int i = 0; i < chunked_array->num_chunks(); i++)
                    {
                        auto chunk = chunked_array->chunk(i);
                        auto array_result =
                            arrow::compute::CallFunction("pairwise_diff", {chunk}, &options);
                        if (!array_result.ok())
                        {
                            throw std::runtime_error("Error calculating diff: " +
                                                     array_result.status().ToString());
                        }
                        result_chunks.push_back(array_result.ValueOrDie().make_array());
                    }

                    auto result = std::make_shared<arrow::ChunkedArray>(result_chunks);
                    if (pad)
                    {
                        return result;
                    }
                    else
                    {
                        auto abs_periods = std::abs(periods);
                        auto nans = factory::array::make_null_array(abs_periods, result->type());
                        bool merge_right = periods < 0;
                        return factory::array::join_chunked_arrays(nans, result, merge_right);
                    }
                })};
        }
    }

    arrow::ChunkedArrayPtr _shift(const arrow::ChunkedArrayPtr& array, int64_t periods)
    {
        // Null-safe: return null if input is null
        if (!array)
        {
            return nullptr;
        }

        // Fast path: zero shift returns the input as-is
        if (periods == 0)
        {
            return array;
        }

        const int64_t length      = array->length();
        const int64_t abs_periods = std::abs(periods);

        // Clamp overshifts to array length to avoid negative slice sizes
        const int64_t clamped = std::min<int64_t>(abs_periods, length);

        // Build the null pad of clamped length
        auto nans = factory::array::make_null_array(clamped, array->type());

        // If fully shifted out, return only nulls of the original length
        if (clamped == length)
        {
            return std::make_shared<arrow::ChunkedArray>(nans);
        }

        // Positive periods -> pad on left (join_right = true) and slice head
        if (periods > 0)
        {
            return factory::array::join_chunked_arrays(nans, array->Slice(0, length - clamped),
                                                       true);
        }

        // Negative periods -> pad on right (join_right = false) and slice tail
        return factory::array::join_chunked_arrays(nans, array->Slice(clamped, length - clamped),
                                                   false);
    }

    TableOrArray shift(const TableOrArray& table, int64_t periods)
    {
        if (table.is_table())
        {
            return TableOrArray{apply_function_to_table(
                table.table(), [&](const arrow::Datum& arr, std::string const&)
                { return _shift(arr.chunked_array(), periods); })};
        }
        return TableOrArray{_shift(table.chunked_array(), periods)};
    }

    arrow::ChunkedArrayPtr _pct_change(const arrow::ChunkedArrayPtr& array, int64_t periods)
    {
        auto shifted     = _shift(array, periods);
        auto diff_result = diff(TableOrArray{array}, periods, true);
        return call_compute_array({diff_result.datum(), arrow::Datum(shifted)}, "divide");
    }

    TableOrArray pct_change(const TableOrArray& table, int64_t periods)
    {
        if (table.is_table())
        {
            return TableOrArray{apply_function_to_table(
                table.table(), [&](const arrow::Datum& arr, std::string const&)
                { return _pct_change(arr.chunked_array(), periods); })};
        }
        return TableOrArray{_pct_change(table.chunked_array(), periods)};
    }

    arrow::ScalarPtr cov(const arrow::ChunkedArrayPtr& array, const arrow::ChunkedArrayPtr& other,
                         std::optional<int64_t> min_periods, int64_t ddof)
    {
        AssertFromFormat(array->length() == other->length(),
                         "covariance: array and other must have the same length");

        // Convert chunked arrays to contiguous arrays for easier processing
        auto x_array = AssertContiguousArrayResultIsOk(arrow::Concatenate(array->chunks()));
        auto y_array = AssertContiguousArrayResultIsOk(arrow::Concatenate(other->chunks()));

        // Calculate means
        arrow::compute::ScalarAggregateOptions agg_options(/*skip_nulls=*/true,
                                                           /*min_count=*/min_periods.value_or(1));
        auto x_mean = AssertCastScalarResultIsOk<arrow::DoubleScalar>(
            call_unary_agg_compute(x_array, "mean", agg_options));
        auto y_mean = AssertCastScalarResultIsOk<arrow::DoubleScalar>(
            call_unary_agg_compute(y_array, "mean", agg_options));

        if (!x_mean.is_valid || !y_mean.is_valid)
        {
            // Return null scalar if we can't compute means
            return std::make_shared<arrow::DoubleScalar>(); // Invalid/null scalar
        }

        // Calculate (x - mean_x) for each element
        auto x_centered =
            call_compute_array({arrow::Datum(x_array), arrow::Datum(x_mean)}, "subtract");

        // Calculate (y - mean_y) for each element
        auto y_centered =
            call_compute_array({arrow::Datum(y_array), arrow::Datum(y_mean)}, "subtract");

        // Calculate (x - mean_x) * (y - mean_y)
        auto products =
            call_compute_array({arrow::Datum(x_centered), arrow::Datum(y_centered)}, "multiply");

        // Calculate sum of products
        auto sum = AssertCastScalarResultIsOk<arrow::DoubleScalar>(
            call_unary_agg_compute(products, "sum", agg_options));

        if (!sum.is_valid)
        {
            return std::make_shared<arrow::DoubleScalar>(); // Invalid/null scalar
        }

        // Count valid pairs
        arrow::compute::CountOptions count_options;
        count_options.mode = arrow::compute::CountOptions::ONLY_VALID;
        auto valid_count   = AssertCastScalarResultIsOk<arrow::Int64Scalar>(
            call_unary_agg_compute(products, "count", count_options));

        // Calculate covariance: sum / (count - ddof)
        double cov_value = sum.value / (valid_count.value - ddof);

        // Return the covariance as a scalar
        return std::make_shared<arrow::DoubleScalar>(cov_value);
    }

    arrow::ScalarPtr corr(const arrow::ChunkedArrayPtr& array, const arrow::ChunkedArrayPtr& other,
                          std::optional<int64_t> min_periods, int64_t ddof)
    {
        AssertFromFormat(array->length() == other->length(),
                         "correlation: array and other must have the same length");

        // Default min_periods if not specified
        int64_t min_p = min_periods.value_or(1);

        // Calculate covariance
        auto cov_scalar = cov(array, other, min_p, ddof);

        // Calculate variances for each array
        auto var_x_scalar = cov(array, array, min_p, ddof);
        auto var_y_scalar = cov(other, other, min_p, ddof);

        // Check if all scalars are valid
        if (!cov_scalar->is_valid || !var_x_scalar->is_valid || !var_y_scalar->is_valid)
        {
            return std::make_shared<arrow::DoubleScalar>(); // Return null scalar
        }

        // Extract values from the scalars
        auto cov_val   = static_cast<arrow::DoubleScalar*>(cov_scalar.get())->value;
        auto var_x_val = static_cast<arrow::DoubleScalar*>(var_x_scalar.get())->value;
        auto var_y_val = static_cast<arrow::DoubleScalar*>(var_y_scalar.get())->value;

        // Calculate Pearson correlation
        double corr_val;
        if (var_x_val <= 0 || var_y_val <= 0)
        {
            return arrow::MakeNullScalar(arrow::float64());
        }
        else
        {
            corr_val = cov_val / std::sqrt(var_x_val * var_y_val);
        }

        return std::make_shared<arrow::DoubleScalar>(corr_val);
    }

} // namespace epoch_frame::arrow_utils
