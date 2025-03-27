//
// Created by adesola on 2/13/25.
//

#include "select.h"
#include "arrow/api.h"
#include "epoch_frame/index.h"
#include "common/methods_helper.h"
#include "epoch_frame/dataframe.h"
#include "epoch_frame/series.h"

namespace epoch_frame {

    TableComponent
    Selections::filter(arrow::ChunkedArrayPtr const &_filter, arrow::compute::FilterOptions const & option) const {
        return TableComponent{m_data.first->filter(Array(AssertContiguousArrayResultIsOk(_filter))),
            arrow_utils::call_compute_table_or_array(m_data.second, std::vector<arrow::Datum>{arrow::Datum{_filter}},
                "filter", &option)};
    }

    TableComponent
    Selections::take(arrow::ArrayPtr const &indices,
            arrow::compute::TakeOptions const &option) const {
        return itake(m_data.first->loc(Array(indices))->array().value(), option);
    }

    TableComponent
    Selections::itake(arrow::ArrayPtr const &integer_indexes,
        arrow::compute::TakeOptions const &option) const {
        return TableComponent{m_data.first->take(Array(integer_indexes)),
        arrow_utils::call_compute_table_or_array(m_data.second, std::vector<arrow::Datum>{arrow::Datum{integer_indexes}},
        "take", &option)};
    }

    TableComponent
    Selections::take(IndexPtr const &new_index,
    arrow::compute::TakeOptions const &option) const {
        auto locations = m_data.first->get_loc(new_index);
        return TableComponent{new_index,
        arrow_utils::call_compute_table_or_array(m_data.second, std::vector{arrow::Datum{factory::array::make_contiguous_array(locations)}},
        "take", &option)};
    }

    TableComponent Selections::drop_null(epoch_frame::DropMethod how,
                                        epoch_frame::AxisType axis,
                                        const std::vector<std::string> &subset,
                                        bool ignore_index) const {
       AssertFromStream(how == DropMethod::Any, "drop_null only support any");
       AssertFromStream(axis == AxisType::Row, "drop_null only support row");
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
            auto filter_ = AssertArrayResultIsOk(arrow::compute::Invert(m_data.first->isin(Array(index)).value()));
            return filter(filter_, arrow::compute::FilterOptions{arrow::compute::FilterOptions::NullSelectionBehavior::DROP});
        }

        return data;
   }

   TableOrArray Selections::fill_null(arrow::ScalarPtr const& value, AxisType axis) const {
    AssertFromFormat(axis == AxisType::Row, "fill_null only supports row-wise filling");
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
                        AssertFromStream(m_data.first->equals(_variant.index()), "IndexMismatch: validation failed.");
                        return arrow::Datum{_variant.table()};
                    }
                    else if constexpr (std::is_same_v<T, DataFrameToSeriesCallable>) {
                        auto series = _variant(DataFrame(m_data.first, m_data.second.table()));
                        AssertFromStream(m_data.first->equals(series.index()), "IndexMismatch: validation failed.");
                        return arrow::Datum{series.array()};
                    }
                    else if constexpr (std::is_same_v<T, DataFrameToDataFrameCallable>) {
                        auto df = _variant(DataFrame(m_data.first, m_data.second.table()));
                        AssertFromStream(m_data.first->equals(df.index()), "IndexMismatch: validation failed.");
                        return arrow::Datum{df.table()};
                    }
                    else if constexpr (std::is_same_v<T, Series>) {
                        AssertFromStream(m_data.first->equals(_variant.index()), "IndexMismatch: validation failed.");
                        return arrow::Datum{_variant.array()};
                    }
                    else if constexpr (std::is_same_v<T, Scalar>) {
                        return arrow::Datum(_variant.value());
                    }
                    else if constexpr (std::is_same_v<T, Array>) {
                        AssertFromStream((m_data.first->size() == static_cast<size_t>(_variant.length())), "ArrayLengthMismatch: validation failed.");
                        return arrow::Datum(_variant.value());
                    }
                    else if constexpr (std::is_same_v<T, arrow::TablePtr>) {
                        AssertFromStream(m_data.second.is_table(), "TableExpected: validation failed.");
                        AssertFromStream(m_data.second.table()->schema()->Equals(_variant->schema()), "TableSchemaMismatch: validation failed.");
                        AssertFromStream(m_data.first->size() == _variant->num_rows(), "TableRowCountMismatch: validation failed.");
                        return arrow::Datum(_variant);
                    }
                    else {
                        static_assert(std::is_same_v<T, DataFrame>, "Invalid variant type");
                    }
                };

                arrow::Datum other_ = std::visit(variant_visitor, other);
                arrow::Datum condition = std::visit(variant_visitor, cond);

                if (m_data.second.is_table()) {
                    return TableOrArray{epoch_frame::arrow_utils::apply_function_to_table(m_data.second.table(), [&](const arrow::Datum &column, std::string const& column_name) {
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

    auto complete_sort = [](IndexPtr const& index, TableOrArray const& data, arrow::Datum const& key, const auto* options) {
        auto sort_indices = arrow_utils::call_unary_compute_contiguous_array(key, "array_sort_indices", options);
        auto sorted_index = arrow_utils::call_compute_contiguous_array({index->array().value(), sort_indices}, "take");
        auto sorted_values = arrow_utils::call_compute_table_or_array(data, {sort_indices}, "take");
        return TableComponent{
            index->Make(sorted_index), sorted_values
    };
    };

    TableComponent Selections::sort_index(bool place_na_last, bool ascending) const {
        using namespace arrow::compute;
        arrow::compute::ArraySortOptions options{
            ascending ? arrow::compute::SortOrder::Ascending : arrow::compute::SortOrder::Descending,
                        place_na_last ? arrow::compute::NullPlacement::AtEnd : arrow::compute::NullPlacement::AtStart
                    };
        return complete_sort(m_data.first, m_data.second, m_data.first->array().value(), &options);
    }

    TableComponent
    Selections::sort_values(std::vector<std::string> const &by, bool place_na_last, bool ascending) const {
        using namespace arrow::compute;
        NullPlacement null_placement = place_na_last ? NullPlacement::AtEnd : NullPlacement::AtStart;
        SortOrder order = ascending ? SortOrder::Ascending : SortOrder::Descending;

        auto [index, data] = m_data;
        SortOptions options;
        options.null_placement = null_placement;
        for (auto const &key: by) {
            options.sort_keys.push_back(
                    SortKey(key, order));
        }

        return complete_sort(m_data.first, m_data.second, m_data.second.datum(), &options);
    }
}
