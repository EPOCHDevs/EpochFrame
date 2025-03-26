#pragma once

#include <arrow/array/builder_base.h>
#include <arrow/table.h>
#include <arrow/type_fwd.h>

#include "epoch_frame/dataframe.h"
#include "array_factory.h"
#include "index/arrow_index.h"

namespace arrow {
    template <>
struct CTypeTraits<epoch_frame::DateTime> {
        using CType = TimestampType::c_type;
        using ArrayType = TimestampArray;
        using BuilderType = TimestampBuilder;

        static constexpr int64_t bytes_required(int64_t elements) {
            return elements * static_cast<int64_t>(sizeof(int64_t));
        }
        constexpr static bool is_parameter_free = false;
    };
}

namespace epoch_frame {
    DataFrame make_dataframe(arrow::TablePtr const &data);

    DataFrame make_dataframe(IndexPtr const &index, arrow::TablePtr const &data);

    DataFrame make_dataframe(IndexPtr const &index, std::vector<arrow::ChunkedArrayPtr> const &data,
                           std::vector<std::string> const &columnNames);

    DataFrame make_dataframe(IndexPtr const &index, std::vector<std::vector<Scalar>> const &data,
                           std::vector<std::string> const &columnNames, arrow::DataTypePtr const &type);

    DataFrame make_dataframe(IndexPtr const &index, std::vector<std::vector<Scalar>> const &data,
                             arrow::FieldVector const &fields);

    template<typename ColumnT>
    DataFrame make_dataframe(IndexPtr const &index, std::vector<std::vector<ColumnT>> const &data,
                           std::vector<std::string> const &columnNames) {
        arrow::ChunkedArrayVector columns;
        arrow::FieldVector fields;
        for (auto const &[name, column]: std::views::zip(columnNames, data)) {
            std::unique_ptr<typename arrow::CTypeTraits<ColumnT>::BuilderType> columnBuilder;

            if constexpr (std::same_as<ColumnT, DateTime>) {
                columnBuilder = std::make_unique<arrow::TimestampBuilder>(
                    arrow::timestamp(arrow::TimeUnit::NANO, column.empty() ? "" : column.front().tz),
                    arrow::default_memory_pool());
            }
            else {
                columnBuilder = std::make_unique<typename arrow::CTypeTraits<ColumnT>::BuilderType>();
            }
            for (auto const& item: column) {
                if constexpr (std::numeric_limits<ColumnT>::has_quiet_NaN) {
                    if (std::isnan(item)) {
                        AssertStatusIsOk(columnBuilder->AppendNull());
                        continue;
                    }
                }
                if constexpr (std::same_as<ColumnT, DateTime>) {
                    AssertStatusIsOk(columnBuilder->Append(item.timestamp().value));
                }
                else {
                    AssertStatusIsOk(columnBuilder->Append(item));
                }
            }
            arrow::ArrayPtr arr;
            AssertStatusIsOk(columnBuilder->Finish(&arr));
            columns.push_back(factory::array::make_array(arr));
            fields.push_back(arrow::field(name, columnBuilder->type()));
        }
        return make_dataframe(index,
                              arrow::Table::Make(arrow::schema(fields), columns));
    }
}
