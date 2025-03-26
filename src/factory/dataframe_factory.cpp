//
// Created by adesola on 2/13/25.
//

#include "dataframe_factory.h"
#include <arrow/api.h>
#include <epoch_core/macros.h>

#include "common/table_or_array.h"


namespace epoch_frame {
    DataFrame make_dataframe(arrow::TablePtr const &data) {
        return DataFrame(data);
    }

    DataFrame make_dataframe(IndexPtr const &index, arrow::TablePtr const &data) {
        return DataFrame(index, data);
    }

    DataFrame make_dataframe(IndexPtr const &index, std::vector<std::vector<Scalar>> const &data,
                       std::vector<std::string> const &columnNames, arrow::DataTypePtr const &type) {

        arrow::ChunkedArrayVector columns;
        arrow::FieldVector fields;
        for (auto const &[name, column]: std::views::zip(columnNames, data)) {
            auto columnBuilder = MakeBuilder(type).MoveValueUnsafe();
            for (auto const& item: column) {
                if (item.is_null()) {
                    AssertStatusIsOk(columnBuilder->AppendNull());
                }
                else {
                    AssertStatusIsOk(columnBuilder->AppendScalar(*item.value()));
                }
            }

            arrow::ArrayPtr arr;
            AssertStatusIsOk(columnBuilder->Finish(&arr));
            columns.push_back(factory::array::make_array(arr));
            fields.push_back(field(name, columnBuilder->type()));
        }
        return make_dataframe(index,
                              arrow::Table::Make(arrow::schema(fields), columns));
    }

    DataFrame make_dataframe(IndexPtr const &index, std::vector<arrow::ChunkedArrayPtr> const &data, std::vector<std::string> const &columnNames) {
        AssertFromStream(data.size() == columnNames.size(), "Data and column names must have the same size");
        arrow::FieldVector fields;

        for (auto const & [i, name]: std::ranges::views::enumerate(columnNames)) {
            fields.push_back(field(name, data[i]->type()));
        }
        return DataFrame(index, arrow::Table::Make(arrow::schema(fields), data));
    }


    DataFrame make_dataframe(IndexPtr const &index, std::vector<std::vector<Scalar>> const &data,
                             arrow::FieldVector const &fields) {
        arrow::ChunkedArrayVector columns;
        for (auto const &[column, field]: std::views::zip(data, fields)) {
            auto columnBuilder = MakeBuilder(field->type()).MoveValueUnsafe();
            for (auto const& item: column) {
                if (item.is_null()) {
                    AssertStatusIsOk(columnBuilder->AppendNull());
                }
                else {
                    AssertStatusIsOk(columnBuilder->AppendScalar(*item.value()));
                }
            }
            arrow::ArrayPtr arr;
            AssertStatusIsOk(columnBuilder->Finish(&arr));
            columns.push_back(factory::array::make_array(arr));
        }
        return make_dataframe(index,
                              arrow::Table::Make(schema(fields), columns));
    }
}
