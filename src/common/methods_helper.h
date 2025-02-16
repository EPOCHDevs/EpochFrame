//
// Created by adesola on 2/13/25.
//

#pragma once
#include "epochframe/aliases.h"
#include <vector>
#include <arrow/api.h>


namespace epochframe {
    arrow::TablePtr add_column(const arrow::TablePtr &table, const std::string &name,
                               const arrow::ChunkedArrayPtr &array);

    std::pair<TableComponent, TableComponent>
    make_table(arrow::ArrayVector const &chunks, uint64_t num_rows,
               std::shared_ptr<arrow::Schema> const &);

    std::tuple<IndexPtr, arrow::TablePtr, arrow::TablePtr>
    align_by_index_and_columns(const TableComponent &left_table_,
                               const TableComponent &right_table_);

    // Perform the actual column-wise binary op once two RecordBatches
    // are aligned by row.
    arrow::TablePtr
    unsafe_binary_op(const arrow::TablePtr &left_rb,
                     const arrow::TablePtr &right_rb,
                     const std::string &op);

    bool has_unique_type(const arrow::SchemaPtr &schema);

    struct DictionaryEncodeResult {
        std::shared_ptr<arrow::Int32Array> indices;
        arrow::ArrayPtr array;
    };

    DictionaryEncodeResult dictionary_encode(const arrow::ArrayPtr &array);

    struct ValueCountResult {
        std::shared_ptr<arrow::Int64Array> counts;
        arrow::ArrayPtr values;
    };

    ValueCountResult value_counts(const arrow::ArrayPtr &array);
}
