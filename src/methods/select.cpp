//
// Created by adesola on 2/13/25.
//

#include "select.h"
#include "arrow/api.h"
#include "index/index.h"
#include "common/methods_helper.h"
#include "epochframe/dataframe.h"
#include "epochframe/series.h"

namespace epochframe {

    TableComponent
    Selections::filter(arrow::ChunkedArrayPtr const &_filter, arrow::compute::FilterOptions const & option) const {
        return TableComponent{m_data.first->filter(AssertContiguousArrayResultIsOk(_filter)),
            arrow_utils::call_compute_table_or_array(m_data.second, std::vector<arrow::Datum>{arrow::Datum{_filter}},
            fmt::format("{}filter", m_data.second.is_table() ? "" : "array_"), &option)};
    }

    TableComponent
    Selections::take(arrow::ArrayPtr const &indices,
            arrow::compute::TakeOptions const &option) const {
        return itake(m_data.first->loc(indices), option);
    }

    TableComponent
    Selections::itake(arrow::ArrayPtr const &integer_indexes,
        arrow::compute::TakeOptions const &option) const {
        return TableComponent{m_data.first->take(PtrCast<arrow::UInt64Array>(integer_indexes)),
        arrow_utils::call_compute_table_or_array(m_data.second, std::vector<arrow::Datum>{arrow::Datum{integer_indexes}},
        fmt::format("{}take", m_data.second.is_table() ? "" : "array_"), &option)};
    }

    TableComponent Selections::drop_null(epochframe::DropMethod how,
                                        epochframe::AxisType axis,
                                        const std::vector<std::string> &subset,
                                        bool ignore_index) const {
       AssertWithTraceFromStream(how == DropMethod::Any, "drop_null only support any");
       AssertWithTraceFromStream(axis == AxisType::Row, "drop_null only support row");
       AssertFalseFromStream(subset.empty(), "subset can not be empty");
       AssertFalseFromStream(ignore_index, "ignore_index can not be true");

       return unzip_index(arrow_utils::call_unary_compute_table(merge_index(), "drop_null"));
   }

   TableComponent Selections::drop(arrow::ArrayPtr const &index,
                                  StringVector const &columns) const {

        TableComponent data = m_data;
        if (m_data.second.is_table() && !columns.empty()) {
            auto table = m_data.second.table();
            data.second = TableOrArray{AssertTableResultIsOk(table->RenameColumns(columns))};
        }

        if (index->length() != 0) {
            auto filter_ = AssertArrayResultIsOk(arrow::compute::Invert(m_data.first->isin(index)));
            return filter(filter_, arrow::compute::FilterOptions{arrow::compute::FilterOptions::NullSelectionBehavior::DROP});
        }

        return data;
   }

   TableOrArray Selections::fill_null(arrow::ScalarPtr const& value, AxisType axis) const {
    AssertWithTraceFromFormat(axis == AxisType::Row, "fill_null only supports row-wise filling");
    if (m_data.second.is_table()) {
        return TableOrArray{arrow_utils::call_compute_fill_null_table(m_data.second.table(), value)};
    }
    else {
        return TableOrArray{arrow_utils::call_compute_fill_null(m_data.second.datum(), value)};
    }
   }

    TableOrArray Selections::where(const WhereConditionVariant &cond, WhereOtherVariant const &other) const {
                auto variant_visitor = [this]<typename T>(const T & _variant) {
                    if constexpr (std::is_same_v<T, DataFrame>) {
                        AssertWithTraceFromStream(m_data.first->equals(_variant.index()), "IndexMismatch: validation failed.");
                        return arrow::Datum{_variant.table()};
                    }
                    else if constexpr (std::is_same_v<T, DataFrameToSeriesCallable>) {
                        auto series = _variant(DataFrame(m_data.first, m_data.second.table()));
                        AssertWithTraceFromStream(m_data.first->equals(series.index()), "IndexMismatch: validation failed.");
                        return arrow::Datum{series.array()};
                    }
                    else if constexpr (std::is_same_v<T, DataFrameToDataFrameCallable>) {
                        auto df = _variant(DataFrame(m_data.first, m_data.second.table()));
                        AssertWithTraceFromStream(m_data.first->equals(df.index()), "IndexMismatch: validation failed.");
                        return arrow::Datum{df.table()};
                    }
                    else if constexpr (std::is_same_v<T, Series>) {
                        AssertWithTraceFromStream(m_data.first->equals(_variant.index()), "IndexMismatch: validation failed.");
                        return arrow::Datum{_variant.array()};
                    }
                    else if constexpr (std::is_same_v<T, Scalar>) {
                        return arrow::Datum(_variant.value());
                    }
                    else if constexpr (std::is_same_v<T, arrow::ArrayPtr>) {
                        AssertWithTraceFromStream(m_data.first->size() == _variant->length(), "ArrayLengthMismatch: validation failed.");
                        return arrow::Datum(_variant);
                    }
                    else if constexpr (std::is_same_v<T, arrow::TablePtr>) {
                        AssertWithTraceFromStream(m_data.second.is_table(), "TableExpected: validation failed.");
                        AssertWithTraceFromStream(m_data.second.table()->schema()->Equals(_variant->schema()), "TableSchemaMismatch: validation failed.");
                        AssertWithTraceFromStream(m_data.first->size() == _variant->num_rows(), "TableRowCountMismatch: validation failed.");
                        return arrow::Datum(_variant);
                    }
                    else {
                        static_assert(std::is_same_v<T, DataFrame>, "Invalid variant type");
                    }
                };

                arrow::Datum other_ = std::visit(variant_visitor, other);
                arrow::Datum condition = std::visit(variant_visitor, cond);

                if (m_data.second.is_table()) {
                    return TableOrArray{epochframe::arrow_utils::apply_function_to_table(m_data.second.table(), [&](const arrow::Datum &column, std::string const& column_name) {
                           arrow::Datum _condition, _other;
                           if (condition.kind() == arrow::Datum::TABLE) {
                               _condition = get_array(*condition.table(), column_name, *arrow::MakeScalar(false));
                           }
                           else {
                               _condition = condition;
                           }

                           if (other_.kind() == arrow::Datum::TABLE) {
                               _other = get_array(*other_.table(), column_name, *arrow::MakeNullScalar(column.type()));
                           }
                           else {
                               _other = other_;
                           }

                           return AssertResultIsOk(arrow::compute::IfElse(
                               _condition,
                               column,
                               _other
                           ));
                       })};
                }

                return TableOrArray{AssertArrayResultIsOk(arrow::compute::IfElse(condition, m_data.second.datum(), other_))};
            }

    TableComponent Selections::sort_index(bool place_na_last, bool ascending) const {
        using namespace arrow::compute;
        SortOptions options;
        options.null_placement = place_na_last ? NullPlacement::AtEnd : NullPlacement::AtStart;
        options.sort_keys.push_back(
                SortKey(RESERVED_INDEX_NAME, ascending ? SortOrder::Ascending : SortOrder::Descending));
        auto sorted_index = arrow_utils::call_unary_compute_table(merge_index(), "sort_indices", &options);
        auto [index, data] = unzip_index(sorted_index);
        return {
                index, data
        };
    }

    TableComponent
    Selections::sort_values(std::vector<std::string> const &by, bool place_na_last, bool ascending) const {
        using namespace arrow::compute;
        NullPlacement null_placement = place_na_last ? NullPlacement::AtEnd : NullPlacement::AtStart;
        SortOrder order = ascending ? SortOrder::Ascending : SortOrder::Descending;

        if (m_data.second.is_table()) {
            SortOptions options;
            options.null_placement = null_placement;
            for (auto const &key: by) {
                options.sort_keys.push_back(
                        SortKey(key, order));
            }
            auto sorted_data = arrow_utils::call_unary_compute_table(m_data.second.table(), "sort_indices", &options);
                    return TableComponent{
                            m_data.first, sorted_data
                    };
        }

        ArraySortOptions options{order, null_placement};
        auto sorted_data = arrow_utils::call_unary_compute_array(m_data.second.chunked_array(), "array_sort_indices", &options);
        return TableComponent{
                m_data.first, sorted_data
        };
    }
}
