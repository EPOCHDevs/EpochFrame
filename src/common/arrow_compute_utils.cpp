//
// Created by adesola on 1/20/25.
//

#include "arrow_compute_utils.h"
#include "index/arrow_index.h"
#include <epoch_lab_shared/macros.h>
#include <arrow/api.h>
#include <vector>
#include <memory>
#include <stdexcept>
#include <fmt/format.h>
#include "epochframe/frame_or_series.h"
#include <tbb/parallel_for.h>
#include <tbb/blocked_range.h>
#include <tbb/task_arena.h>
#include "factory/array_factory.h"

namespace epochframe::arrow_utils {
    IndexPtr integer_slice_index(const IIndex &index, size_t start, size_t end) {
        return index.Make(slice_array(index.array().value(), start, end));
    }

    IndexPtr integer_slice_index(const IIndex &index, size_t start, size_t end, size_t step) {
        return index.Make(slice_array(index.array().value(), start, end, step));
    }

     arrow::ScalarPtr
    call_unary_agg_compute(const arrow::Datum &input, const std::string &function_name,
        arrow::compute::FunctionOptions const & options) {
        const auto datum = call_unary_compute(input, function_name, &options);

        if (datum.is_scalar()) {
            return datum.scalar();
        }
        if (datum.is_array()) {
            const auto arr = datum.make_array();
            AssertFromFormat(arr->length() == 1, "Failed to create Scalar from agg result array");
            return AssertResultIsOk(arr->GetScalar(0));
        }
        return MakeNullScalar(arrow::null());
    }

    arrow::TablePtr apply_function_to_table(const arrow::TablePtr &table, std::function<arrow::Datum(arrow::Datum const&, std::string const&)> func) {
        std::vector<std::shared_ptr<arrow::ChunkedArray>> processed_columns(table->num_columns());
        arrow::FieldVector fields = table->schema()->fields();

        parallel_for(tbb::blocked_range<int64_t>(0, table->num_columns()), [&](const tbb::blocked_range<int64_t>& r) {
            for (int64_t i = r.begin(); i != r.end(); ++i) {
                auto merged = AssertContiguousArrayResultIsOk(arrow::Concatenate(table->column(i)->chunks()));
                auto result = func(arrow::Datum(merged), fields[i]->name());
                if (result.kind() == arrow::Datum::ARRAY) {
                    processed_columns[i] = std::make_shared<arrow::ChunkedArray>(result.make_array());
                    fields[i] = field(fields[i]->name(), processed_columns[i]->type());
                } else if (result.kind() == arrow::Datum::CHUNKED_ARRAY) {
                    processed_columns[i] = result.chunked_array();
                    fields[i] = field(fields[i]->name(), processed_columns[i]->type());
                } else {
                    throw std::runtime_error(std::format("Invalid arrow::Datum kind: {}", arrow::ToString(result.kind())));
                }
            }
        });

        return arrow::Table::Make(arrow::schema(fields), processed_columns);
    }

    // Helper to get a scalar from an array at a specific index
    std::shared_ptr<arrow::Scalar> GetScalar(const std::shared_ptr<arrow::Array>& array, int64_t index) {
        if (!array || index < 0 || index >= array->length()) {
            return nullptr;
        }

        auto result = array->GetScalar(index);
        if (!result.ok()) {
            throw std::runtime_error("Failed to get scalar: " + result.status().ToString());
        }

        return result.ValueOrDie();
    }

    chrono_time_point get_time_point(const Scalar &scalar) {
        return get_time_point(scalar.timestamp());
    }

    arrow::ArrayPtr map(const arrow::ArrayPtr& array,
                             const std::function<Scalar(const Scalar&)>& func,
                             bool ignore_nulls) {
        if (!array) {
            return nullptr;
        }
        AssertFromStream(func, "Function is not callable");

        std::vector<arrow::ScalarPtr> scalars;
        scalars.reserve(array->length());
        const auto null_scalar = arrow::MakeNullScalar(array->type());

        for (int64_t i = 0; i < array->length(); ++i) {
            auto scalar = AssertResultIsOk(array->GetScalar(i));
            if (!scalar->is_valid && ignore_nulls) {
                scalars.push_back(null_scalar);
                continue;
            }

            scalars.push_back(func(Scalar(scalar)).value());
        }

        arrow::DataTypePtr data_type;
        const auto iter = std::ranges::find_if(scalars, [&](const arrow::ScalarPtr& scalar) {
            return scalar->is_valid;
        });
        if (iter == scalars.end()) {
            data_type = array->type();
        }
        else {
            data_type = (*iter)->type;
        }

        auto builder_result = arrow::MakeBuilder(data_type);
        AssertFromFormat(builder_result.ok(), "Failed to create builder for array type");
        auto builder = builder_result.MoveValueUnsafe();

        auto x = builder->AppendScalars(scalars);
        if (x.ok()) {
            return AssertResultIsOk(builder->Finish());
        }
        std::stringstream ss;
        ss << x.message() << "\nValid Scalar Builder:\n";
        for (const auto& scalar : scalars) {
            ss << scalar->ToString() << "\n";
        }
        throw std::runtime_error(ss.str());
    }

    arrow::ChunkedArrayPtr map(const arrow::ChunkedArrayPtr& array,
                               const std::function<Scalar(const Scalar&)>& func,
                               bool ignore_nulls) {
        if (!array) {
            return nullptr;
        }

                    std::vector<std::shared_ptr<arrow::Array>> chunks;

        // Process each chunk in the column
        for (int chunk_idx = 0; chunk_idx < array->num_chunks(); ++chunk_idx) {
            auto chunk = array->chunk(chunk_idx);
            auto mapped_chunk = map(chunk, func, ignore_nulls);
            chunks.push_back(mapped_chunk);
        }

            // Create a chunked array from the mapped chunks
            return std::make_shared<arrow::ChunkedArray>(chunks);
    }

    arrow::TablePtr map(const arrow::TablePtr& table,
                                     const std::function<Scalar(const Scalar&)>& func,
                                     bool ignore_nulls) {
        if (!table) {
            return nullptr;
        }

        // Create a vector to store the result columns
        std::vector<std::shared_ptr<arrow::ChunkedArray>> result_columns;
        result_columns.reserve(table->num_columns());

        // Process each column
        for (int col = 0; col < table->num_columns(); ++col) {
            result_columns.push_back(map(table->column(col), func, ignore_nulls));
        }

        // Create a new table with the original schema and the mapped columns
        return arrow::Table::Make(table->schema(), result_columns);
    }

    chrono_year_month_day get_year_month_day(const arrow::TimestampScalar &scalar) {
        auto ymd = AssertCastScalarResultIsOk<arrow::StructScalar>(arrow::compute::YearMonthDay(scalar));
        return {
            chrono_year(AssertCastScalarResultIsOk<arrow::Int64Scalar>(ymd.field("year")).value),
            chrono_month(AssertCastScalarResultIsOk<arrow::Int64Scalar>(ymd.field("month")).value),
            chrono_day(AssertCastScalarResultIsOk<arrow::Int64Scalar>(ymd.field("day")).value),
        };
    }

    chrono_year get_year(const arrow::TimestampScalar &scalar) {
        auto year = AssertCastScalarResultIsOk<arrow::Int64Scalar>(arrow::compute::Year(scalar));
        return chrono_year(year.value);
    }

    chrono_month get_month(const arrow::TimestampScalar &scalar) {
        auto month = AssertCastScalarResultIsOk<arrow::Int64Scalar>(arrow::compute::Month(scalar));
        return chrono_month(month.value);
    }

    chrono_day get_day(const arrow::TimestampScalar &scalar) {
        auto day = AssertCastScalarResultIsOk<arrow::Int64Scalar>(arrow::compute::Day(scalar));
        return chrono_day(day.value);
    }

    std::chrono::nanoseconds duration(const arrow::TimestampScalar &scalar1, const arrow::TimestampScalar &scalar2) {
        AssertFromFormat(scalar1.type->Equals(scalar2.type), "duration between incompatible timestamp.");
        auto type = std::dynamic_pointer_cast<arrow::TimestampType>(scalar1.type);
        AssertFromFormat(type && type->unit() == arrow::TimeUnit::NANO, "duration only supports nanoseconds.");

        auto diff = scalar1.value - scalar2.value;
        auto n = std::chrono::nanoseconds(diff);
        return n;
    }

    std::string get_tz(const arrow::DataTypePtr &type) {
        AssertFromStream(type, "Type is not valid");

        auto dt = std::dynamic_pointer_cast<arrow::TimestampType>(type);
        if (!dt) {
            return "";  // Return empty string for non-timestamp types instead of asserting
        }

        std::string tz = dt->timezone();

        // Handle empty timezone (naive timestamps)
        if (tz.empty()) {
            return "";
        }

        // Normalize timezone format if needed
        // Some timezone formats may need standardization
        // E.g., "UTC+01:00" vs "+01:00" vs "Etc/GMT-1"

        return tz;
    }

    TableOrArray call_unary_compute_table_or_array(const TableOrArray &table, std::string const &function_name, arrow::compute::FunctionOptions const &options) {
        if (table.is_table()) {
            return TableOrArray{apply_function_to_table(table.table(), [&](const arrow::Datum &arr, std::string const&) {
                return call_unary_compute_array(arr, function_name, &options);
            })};
        }
        return TableOrArray{call_unary_compute_contiguous_array(table.datum(), function_name, &options)};
    }

    TableOrArray call_compute_is_in(const TableOrArray &table, const arrow::ArrayPtr &values) {
        arrow::compute::SetLookupOptions options{values, arrow::compute::SetLookupOptions::NullMatchingBehavior::MATCH};
        return call_unary_compute_table_or_array(table, "is_in", options);
    }

    TableOrArray call_compute_index_in(const TableOrArray &table, const arrow::ArrayPtr &values) {
        arrow::compute::SetLookupOptions options{values, arrow::compute::SetLookupOptions::NullMatchingBehavior::MATCH};
        return call_unary_compute_table_or_array(table, "index_in", options);
    }
}
