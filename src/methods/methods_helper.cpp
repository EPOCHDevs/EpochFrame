//
// Created by adesola on 2/13/25.
//

#include "methods_helper.h"
#include <fmt/format.h>


namespace epochframe {
    std::shared_ptr<arrow::ChunkedArray> make_chunked_array(arrow::RecordBatchPtr const& recordBatch) {
        return std::make_shared<arrow::ChunkedArray>(recordBatch->columns());
    }

    arrow::RecordBatchPtr make_record_batch(arrow::ArrayVector const& table,
                                            uint64_t num_rows,
                                            std::shared_ptr<arrow::Schema> const& schema) {
        arrow::ArrayPtr merged;
        if (table.size() == 1) {
            merged = table.at(0);
        }
        else {
            auto result = arrow::Concatenate(table);
            if (!result.ok()) {
                throw std::runtime_error("Failed to Create DataFrame from array list. " + result.status().ToString());
            }
            merged = result.MoveValueUnsafe();
        }

        const uint64_t num_columns = schema->num_fields();
        arrow::ArrayVector batches(num_columns);
        if (merged->length() != num_columns*num_rows) {
            throw std::runtime_error("Failed to Create DataFrame from array list. array length != rows*columns, " +
                                     fmt::format("{} != {}", merged->length(), num_columns * num_rows));
        }

        for (auto i = 0; i < num_columns; i++) {
            auto start = i * num_rows;
            batches[i] = merged->Slice(start, num_rows);
        }

        return arrow::RecordBatch::Make(schema, num_rows, batches);
    }
}
