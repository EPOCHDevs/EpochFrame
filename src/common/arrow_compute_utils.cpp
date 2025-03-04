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


namespace epochframe::arrow_utils {
    IndexPtr integer_slice_index(const Index &index, size_t start, size_t end) {
        return index.Make(slice_array(index.array(), start, end));
    }

    IndexPtr integer_slice_index(const Index &index, size_t start, size_t end, size_t step) {
        return index.Make(slice_array(index.array(), start, end, step));
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
            AssertWithTraceFromFormat(arr->length() == 1, "Failed to create Scalar from agg result array");
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
                    throw std::runtime_error(fmt::format("Invalid arrow::Datum kind: {}", arrow::ToString(result.kind())));
                }
            }
        });

        return arrow::Table::Make(arrow::schema(fields), processed_columns);
    }
}
