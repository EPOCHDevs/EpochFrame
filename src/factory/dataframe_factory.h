//
// Created by adesola on 2/13/25.
//

#pragma once

#include <arrow/array/builder_base.h>
#include <arrow/table.h>
#include <range/v3/view/zip.hpp>
#include "epochframe/ndframe.h"
#include "array_factory.h"
#include "index/arrow_index.h"

namespace epochframe {
    NDFrame make_dataframe(arrow::TablePtr const &data);

    NDFrame make_dataframe(IndexPtr const &index, arrow::TablePtr const &data);

    template<typename ColumnT>
    NDFrame make_dataframe(IndexPtr const &index, std::vector<std::vector<ColumnT>> const &data,
                           std::vector<std::string> const &columnNames) {
        arrow::ChunkedArrayVector columns;
        arrow::FieldVector fields;
        for (auto const &[name, column]: ranges::view::zip(columnNames, data)) {
            typename arrow::CTypeTraits<ColumnT>::BuilderType columnBuilder;
            columnBuilder.AppendValues(column);

            arrow::ArrayPtr arr;
            columnBuilder.Finish(&arr);
            columns.push_back(factory::array::MakeArray(arr));
            fields.push_back(arrow::field(name, columnBuilder.type()));
        }
        return make_dataframe(index,
                              arrow::Table::Make(arrow::schema(fields), columns));
    }
}
