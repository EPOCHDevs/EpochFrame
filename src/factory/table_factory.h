#pragma once

#include <arrow/api.h>
#include <epoch_core/macros.h>
#include "epochframe/aliases.h"


namespace epochframe::factory::table {

// Creates an empty arrow Table with the given schema (0 rows).
arrow::TablePtr make_empty_table(const arrow::SchemaPtr& schema);

arrow::ChunkedArrayPtr make_empty_chunked_array(const arrow::DataTypePtr& type=arrow::null());

arrow::TablePtr make_null_table(const arrow::SchemaPtr& schema, int64_t num_rows);

template<typename T>
std::shared_ptr<T> make_empty_table_or_array() {
    if constexpr (std::is_same_v<T, arrow::Table>) {
        return make_empty_table(arrow::schema({}));
    } else if constexpr (std::is_same_v<T, arrow::ChunkedArray>) {
        return make_empty_chunked_array();
    } else {
        static_assert(std::is_same_v<T, arrow::Table> || std::is_same_v<T, arrow::ChunkedArray>, "Unsupported type");
    }
    return nullptr;
}

TableOrArray make_empty_table_or_array(TableOrArray const& tableOrArray);

TableOrArray make_table_or_array(arrow::TablePtr const& table, const std::string& series_name);

template<typename T>
int64_t get_size(std::shared_ptr<T> const& arr) {
    AssertFromFormat(arr, "get_size failed due to arr == nullptr.");
    if constexpr (std::is_same_v<T, arrow::Table>) {
        return arr->num_rows();
    } else if constexpr (std::is_same_v<T, arrow::ChunkedArray>) {
        return arr->length();
    } else {
        static_assert(std::is_same_v<T, arrow::Table> || std::is_same_v<T, arrow::ChunkedArray>, "Unsupported type");
    }
}

} // namespace epochframe::factory::table
