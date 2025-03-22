#include "table_factory.h"
#include <stdexcept>
#include "factory/array_factory.h"
#include <epoch_core/macros.h>
#include "common/table_or_array.h"


namespace epoch_frame::factory::table {

arrow::ChunkedArrayPtr make_empty_chunked_array(const arrow::DataTypePtr& type) {
    return std::make_shared<arrow::ChunkedArray>(std::vector<arrow::ArrayPtr>{}, type);
}

arrow::TablePtr make_null_table(const arrow::SchemaPtr& schema, int64_t num_rows) {
    std::vector<arrow::ChunkedArrayPtr> columns(schema->num_fields());
    std::ranges::transform(schema->fields(), columns.begin(), [&](const arrow::FieldPtr& field) {
        return AssertArrayResultIsOk(arrow::MakeArrayOfNull(field->type(), num_rows));
    });

    return AssertTableResultIsOk(arrow::Table::Make(schema, columns));
}

arrow::TablePtr make_empty_table(const arrow::SchemaPtr& schema) {
    if (!schema) {
        throw std::invalid_argument("Schema cannot be null");
    }

    const auto num_fields = schema->num_fields();
    std::vector<std::shared_ptr<arrow::ChunkedArray>> columns;
    columns.reserve(num_fields);

    for (int i = 0; i < num_fields; ++i) {
        auto field = schema->field(i);
        auto array_result = arrow::MakeArrayOfNull(field->type(), 0);
        auto array = AssertResultIsOk(array_result);
        columns.push_back(factory::array::make_array(array));
    }

    return arrow::Table::Make(schema, columns);
}

TableOrArray make_empty_table_or_array(TableOrArray const& tableOrArray) {
    if  (tableOrArray.is_table()) {
        return TableOrArray(make_empty_table(tableOrArray.table()->schema()));
    }
    if (tableOrArray.is_chunked_array()) {
        return TableOrArray(make_empty_chunked_array(tableOrArray.chunked_array()->type()));
    }
    throw std::invalid_argument("Unsupported type");
}

TableOrArray make_table_or_array(arrow::TablePtr const& table, const std::string& series_name) {
    if (table->num_columns() == 1 && table->field(0)->name() == series_name) {
        return TableOrArray(table->column(0));
    }
    return TableOrArray(table);
}

} // namespace epoch_frame::factory::table
