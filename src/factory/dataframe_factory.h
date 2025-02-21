//
// Created by adesola on 2/13/25.
//

#pragma once

#include <arrow/array/builder_base.h>
#include <arrow/table.h>
#include <range/v3/view/zip.hpp>
#include "epochframe/dataframe.h"
#include "array_factory.h"
#include "index/arrow_index.h"

namespace epochframe {
    DataFrame make_dataframe(arrow::TablePtr const &data);

    DataFrame make_dataframe(IndexPtr const &index, arrow::TablePtr const &data);

    template<typename ColumnT>
    DataFrame make_dataframe(IndexPtr const &index, std::vector<std::vector<ColumnT>> const &data,
                           std::vector<std::string> const &columnNames) {
        arrow::ChunkedArrayVector columns;
        arrow::FieldVector fields;
        for (auto const &[name, column]: ranges::view::zip(columnNames, data)) {
            typename arrow::CTypeTraits<ColumnT>::BuilderType columnBuilder;
            AssertStatusIsOk(columnBuilder.AppendValues(column));

            arrow::ArrayPtr arr;
            AssertStatusIsOk(columnBuilder.Finish(&arr));
            columns.push_back(factory::array::make_array(arr));
            fields.push_back(arrow::field(name, columnBuilder.type()));
        }
        return make_dataframe(index,
                              arrow::Table::Make(arrow::schema(fields), columns));
    }
}
