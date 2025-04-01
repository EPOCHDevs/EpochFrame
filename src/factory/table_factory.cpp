#include "epoch_frame/factory/table_factory.h"
#include <stdexcept>
#include "epoch_frame/factory/array_factory.h"
#include <epoch_core/macros.h>
#include "common/table_or_array.h"


namespace epoch_frame::factory::table {

arrow::ChunkedArrayPtr make_empty_chunked_array(const arrow::DataTypePtr& type) {
    return std::make_shared<arrow::ChunkedArray>(std::vector<arrow::ArrayPtr>{}, type);
}

arrow::ChunkedArrayPtr make_null_chunked_array(const arrow::DataTypePtr& type, int64_t length) {
    return std::make_shared<arrow::ChunkedArray>(AssertResultIsOk(arrow::MakeArrayOfNull(type, length)));
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

arrow::TablePtr make_table(std::vector<std::vector<Scalar>> const& data, std::vector<std::string> const& names, arrow::DataTypePtr const& type){
    AssertFromFormat(data.size() == names.size(), "make_table: data and names must have the same size");
    arrow::FieldVector fields;
    fields.reserve(names.size());
    for (size_t i = 0; i < names.size(); ++i) {
        fields.push_back(arrow::field(names[i], type));
    }
    return make_table(data, fields);
}

arrow::TablePtr make_table(std::vector<std::vector<Scalar>> const& data, arrow::FieldVector const& fields){
    AssertFromFormat(data.size() == fields.size(), "make_table: data and fields must have the same size");
    std::vector<arrow::ChunkedArrayPtr> columns;
    columns.reserve(fields.size());
    for (size_t i = 0; i < fields.size(); ++i) {
        columns.push_back(factory::array::make_chunked_array(data[i], fields[i]->type()));
    }
    return arrow::Table::Make(arrow::schema(fields), columns);
}

} // namespace epoch_frame::factory::table
