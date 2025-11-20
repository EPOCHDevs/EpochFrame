//
// Created by adesola on 2/13/25.
//

#include "select.h"
#include "arrow/api.h"
#include "common/methods_helper.h"
#include "epoch_core/macros.h"
#include "epoch_frame/dataframe.h"
#include "epoch_frame/factory/index_factory.h"
#include "epoch_frame/index.h"
#include "epoch_frame/series.h"

namespace epoch_frame
{

    TableComponent Selections::filter(arrow::ChunkedArrayPtr const&        _filter,
                                      arrow::compute::FilterOptions const& option) const
    {
        return TableComponent{m_data.first->filter(Array(AssertContiguousArrayResultIsOk(_filter))),
                              arrow_utils::call_compute_table_or_array(
                                  m_data.second, std::vector<arrow::Datum>{arrow::Datum{_filter}},
                                  "filter", &option)};
    }

    TableComponent Selections::take(arrow::ArrayPtr const&             indices,
                                    arrow::compute::TakeOptions const& option) const
    {
        return itake(m_data.first->loc(Array(indices))->array().value(), option);
    }

    TableComponent Selections::itake(arrow::ArrayPtr const&             integer_indexes,
                                     arrow::compute::TakeOptions const& option) const
    {
        return TableComponent{m_data.first->take(Array(integer_indexes)),
                              arrow_utils::call_compute_table_or_array(
                                  m_data.second,
                                  std::vector<arrow::Datum>{arrow::Datum{integer_indexes}}, "take",
                                  &option)};
    }

    TableComponent Selections::take(IndexPtr const&                    new_index,
                                    arrow::compute::TakeOptions const& option) const
    {
        auto locations = m_data.first->get_loc(new_index);
        auto data      = arrow_utils::call_compute_table_or_array(
            m_data.second,
            std::vector{arrow::Datum{factory::array::make_contiguous_array(locations)}}, "take",
            &option);
        AssertFromFormat(new_index->size() == static_cast<size_t>(data.size()),
                         "IndexLengthMismatch: validation failed.");
        return TableComponent{new_index, data};
    }

    TableComponent Selections::drop_null(epoch_frame::DropMethod             how,
                                         epoch_frame::AxisType                axis,
                                         std::optional<int64_t> const&        thresh,
                                         const std::vector<std::string>&      subset,
                                         bool                                 ignore_index) const
    {
        // Validate that thresh and how are not both specified
        AssertFromFormat(!thresh.has_value() || how == DropMethod::Any,
                         "Cannot specify both 'thresh' and 'how' (must use default 'any')");

        if (axis == AxisType::Row)
        {
            // For Series (ChunkedArray), use filter-based approach since Arrow's drop_null for ChunkedArray
            // doesn't preserve the index
            if (!m_data.second.is_table())
            {
                AssertFromFormat(!thresh.has_value(), "thresh parameter not supported for Series");
                AssertFromFormat(subset.empty(), "subset parameter not supported for Series");

                // Create a boolean filter where values are valid (not null)
                auto is_valid_result = arrow_utils::call_unary_compute_table_or_array(m_data.second, "is_valid");
                auto is_valid_array = AssertContiguousArrayResultIsOk(is_valid_result.chunked_array());

                // Use the filter method which handles index properly
                auto result = filter(std::make_shared<arrow::ChunkedArray>(is_valid_array),
                                   arrow::compute::FilterOptions::Defaults());

                if (ignore_index)
                {
                    return TableComponent{factory::index::from_range(result.second.size()),
                                          result.second};
                }
                return result;
            }

            // For row-wise dropping on DataFrames
            arrow::ChunkedArrayPtr filter_mask;

            if (thresh.has_value())
            {
                // Count non-null values and keep rows with at least thresh non-nulls
                // Use subset columns if specified, otherwise all columns
                arrow::TablePtr table_to_check = m_data.second.table();

                if (!subset.empty())
                {
                    // Select only the subset columns
                    std::vector<std::shared_ptr<arrow::Field>> subset_fields;
                    std::vector<std::shared_ptr<arrow::ChunkedArray>> subset_columns;

                    for (const auto& col_name : subset)
                    {
                        auto col_idx = table_to_check->schema()->GetFieldIndex(col_name);
                        AssertFromFormat(col_idx != -1, "Column '{}' not found in table", col_name);
                        subset_fields.push_back(table_to_check->schema()->field(col_idx));
                        subset_columns.push_back(table_to_check->column(col_idx));
                    }

                    table_to_check =
                        AssertTableResultIsOk(arrow::Table::Make(arrow::schema(subset_fields), subset_columns));
                }

                // Count non-nulls per row
                auto num_rows = table_to_check->num_rows();
                auto num_cols = table_to_check->num_columns();

                // Build a count array of non-null values per row
                arrow::Int64Builder count_builder;
                AssertStatusIsOk(count_builder.Reserve(num_rows));

                for (int64_t row = 0; row < num_rows; ++row)
                {
                    int64_t non_null_count = 0;
                    for (int col = 0; col < num_cols; ++col)
                    {
                        auto chunked_array = table_to_check->column(col);
                        auto scalar        = AssertScalarResultIsOk(chunked_array->GetScalar(row));
                        if (scalar->is_valid)
                        {
                            non_null_count++;
                        }
                    }
                    AssertStatusIsOk(count_builder.Append(non_null_count));
                }

                auto count_array = AssertArrayResultIsOk(count_builder.Finish());

                // Create filter: count >= thresh
                auto thresh_scalar = arrow::MakeScalar(thresh.value());
                auto filter_result = arrow::compute::CallFunction("greater_equal", {count_array, thresh_scalar});
                filter_mask = factory::array::make_chunked_array(filter_result);
            }
            else
            {
                // Use how parameter (Any or All)
                arrow::TablePtr table_to_check = m_data.second.table();

                if (!subset.empty())
                {
                    // Select only the subset columns
                    std::vector<std::shared_ptr<arrow::Field>> subset_fields;
                    std::vector<std::shared_ptr<arrow::ChunkedArray>> subset_columns;

                    for (const auto& col_name : subset)
                    {
                        auto col_idx = table_to_check->schema()->GetFieldIndex(col_name);
                        AssertFromFormat(col_idx != -1, "Column '{}' not found in table", col_name);
                        subset_fields.push_back(table_to_check->schema()->field(col_idx));
                        subset_columns.push_back(table_to_check->column(col_idx));
                    }

                    table_to_check =
                        AssertTableResultIsOk(arrow::Table::Make(arrow::schema(subset_fields), subset_columns));
                }

                if (how == DropMethod::Any)
                {
                    // Drop rows with ANY null values
                    // Use Arrow's built-in drop_null if no subset is specified
                    if (subset.empty())
                    {
                        auto result = unzip_index(arrow_utils::call_unary_compute_table(merge_index(), "drop_null"));
                        if (ignore_index)
                        {
                            return TableComponent{factory::index::from_range(result.second.size()),
                                                  result.second};
                        }
                        return result;
                    }

                    // Build filter mask: all columns must be valid
                    auto num_rows = table_to_check->num_rows();
                    auto num_cols = table_to_check->num_columns();

                    arrow::BooleanBuilder filter_builder;
                    AssertStatusIsOk(filter_builder.Reserve(num_rows));

                    for (int64_t row = 0; row < num_rows; ++row)
                    {
                        bool all_valid = true;
                        for (int col = 0; col < num_cols; ++col)
                        {
                            auto chunked_array = table_to_check->column(col);
                            auto scalar        = AssertScalarResultIsOk(chunked_array->GetScalar(row));
                            if (!scalar->is_valid)
                            {
                                all_valid = false;
                                break;
                            }
                        }
                        AssertStatusIsOk(filter_builder.Append(all_valid));
                    }

                    auto filter_array = AssertResultIsOk(filter_builder.Finish());
                    arrow::ArrayVector vec = {filter_array};
                    filter_mask = AssertResultIsOk(arrow::ChunkedArray::Make(vec));
                }
                else // DropMethod::All
                {
                    // Drop rows where ALL values are null
                    auto num_rows = table_to_check->num_rows();
                    auto num_cols = table_to_check->num_columns();

                    arrow::BooleanBuilder filter_builder;
                    AssertStatusIsOk(filter_builder.Reserve(num_rows));

                    for (int64_t row = 0; row < num_rows; ++row)
                    {
                        bool any_valid = false;
                        for (int col = 0; col < num_cols; ++col)
                        {
                            auto chunked_array = table_to_check->column(col);
                            auto scalar        = AssertScalarResultIsOk(chunked_array->GetScalar(row));
                            if (scalar->is_valid)
                            {
                                any_valid = true;
                                break;
                            }
                        }
                        AssertStatusIsOk(filter_builder.Append(any_valid));
                    }

                    auto filter_array = AssertResultIsOk(filter_builder.Finish());
                    arrow::ArrayVector vec = {filter_array};
                    filter_mask = AssertResultIsOk(arrow::ChunkedArray::Make(vec));
                }
            }

            // Apply filter
            auto result = filter(filter_mask, arrow::compute::FilterOptions::Defaults());

            if (ignore_index)
            {
                return TableComponent{factory::index::from_range(result.second.size()),
                                      result.second};
            }

            return result;
        }
        else // AxisType::Column
        {
            // For column-wise dropping (only makes sense for DataFrames)
            AssertFromFormat(m_data.second.is_table(), "Column-wise drop_null requires a DataFrame");
            AssertFromFormat(subset.empty(), "subset parameter not supported for column-wise drop_null");

            auto table = m_data.second.table();
            std::vector<std::shared_ptr<arrow::Field>> kept_fields;
            std::vector<std::shared_ptr<arrow::ChunkedArray>> kept_columns;

            for (int col = 0; col < table->num_columns(); ++col)
            {
                auto chunked_array = table->column(col);
                bool should_keep   = false;

                if (thresh.has_value())
                {
                    // Count non-nulls in this column
                    int64_t non_null_count = chunked_array->length() - chunked_array->null_count();
                    should_keep            = non_null_count >= thresh.value();
                }
                else if (how == DropMethod::Any)
                {
                    // Keep column if it has no nulls
                    should_keep = chunked_array->null_count() == 0;
                }
                else // DropMethod::All
                {
                    // Keep column if not all values are null
                    should_keep = chunked_array->null_count() < chunked_array->length();
                }

                if (should_keep)
                {
                    kept_fields.push_back(table->schema()->field(col));
                    kept_columns.push_back(chunked_array);
                }
            }

            auto new_table =
                AssertTableResultIsOk(arrow::Table::Make(arrow::schema(kept_fields), kept_columns));

            return TableComponent{m_data.first, TableOrArray{new_table}};
        }
    }

    TableComponent Selections::drop(arrow::ArrayPtr const& index, StringVector const& columns) const
    {

        TableComponent data = m_data;
        if (m_data.second.is_table() && !columns.empty())
        {
            auto table  = m_data.second.table();
            data.second = TableOrArray{AssertTableResultIsOk(table->RenameColumns(columns))};
        }

        if (index->length() != 0)
        {
            auto filter_ = AssertArrayResultIsOk(
                arrow::compute::Invert(m_data.first->isin(Array(index)).value()));
            return filter(filter_, arrow::compute::FilterOptions{
                                       arrow::compute::FilterOptions::NullSelectionBehavior::DROP});
        }

        return data;
    }

    TableOrArray Selections::fill_null(arrow::ScalarPtr const& value, AxisType axis) const
    {
        AssertFromFormat(axis == AxisType::Row, "fill_null only supports row-wise filling");
        if (m_data.second.is_table())
        {
            return TableOrArray{
                arrow_utils::call_compute_fill_null_table(m_data.second.table(), value)};
        }
        else
        {
            return TableOrArray{arrow_utils::call_compute_fill_null(m_data.second.datum(), value)};
        }
    }

    TableOrArray Selections::where(const WhereConditionVariant& cond,
                                   WhereOtherVariant const&     other) const
    {
        auto variant_visitor = [this]<typename T>(const T& _variant)
        {
            if constexpr (std::is_same_v<T, DataFrame>)
            {
                AssertFromStream(m_data.first->equals(_variant.index()),
                                 "IndexMismatch: validation failed.");
                return arrow::Datum{_variant.table()};
            }
            else if constexpr (std::is_same_v<T, DataFrameToSeriesCallable>)
            {
                auto series = _variant(DataFrame(m_data.first, m_data.second.table()));
                AssertFromStream(m_data.first->equals(series.index()),
                                 "IndexMismatch: validation failed.");
                return arrow::Datum{series.array()};
            }
            else if constexpr (std::is_same_v<T, DataFrameToDataFrameCallable>)
            {
                auto df = _variant(DataFrame(m_data.first, m_data.second.table()));
                AssertFromStream(m_data.first->equals(df.index()),
                                 "IndexMismatch: validation failed.");
                return arrow::Datum{df.table()};
            }
            else if constexpr (std::is_same_v<T, Series>)
            {
                AssertFromStream(m_data.first->equals(_variant.index()),
                                 "IndexMismatch: validation failed.");
                return arrow::Datum{_variant.array()};
            }
            else if constexpr (std::is_same_v<T, Scalar>)
            {
                return arrow::Datum(_variant.value());
            }
            else if constexpr (std::is_same_v<T, Array>)
            {
                AssertFromStream((m_data.first->size() == static_cast<size_t>(_variant.length())),
                                 "ArrayLengthMismatch: validation failed.");
                return arrow::Datum(_variant.value());
            }
            else if constexpr (std::is_same_v<T, arrow::TablePtr>)
            {
                AssertFromStream(m_data.second.is_table(), "TableExpected: validation failed.");
                AssertFromStream(m_data.second.table()->schema()->Equals(_variant->schema()),
                                 "TableSchemaMismatch: validation failed.");
                AssertFromStream(m_data.first->size() == _variant->num_rows(),
                                 "TableRowCountMismatch: validation failed.");
                return arrow::Datum(_variant);
            }
            else
            {
                static_assert(std::is_same_v<T, DataFrame>, "Invalid variant type");
            }
        };

        arrow::Datum other_    = std::visit(variant_visitor, other);
        arrow::Datum condition = std::visit(variant_visitor, cond);

        if (m_data.second.is_table())
        {
            return TableOrArray{epoch_frame::arrow_utils::apply_function_to_table(
                m_data.second.table(),
                [&](const arrow::Datum& column, std::string const& column_name)
                {
                    arrow::Datum _condition, _other;
                    if (condition.kind() == arrow::Datum::TABLE)
                    {
                        _condition =
                            get_array(*condition.table(), column_name, *arrow::MakeScalar(false));
                    }
                    else
                    {
                        _condition = condition;
                    }

                    if (other_.kind() == arrow::Datum::TABLE)
                    {
                        _other = get_array(*other_.table(), column_name,
                                           *arrow::MakeNullScalar(column.type()));
                    }
                    else
                    {
                        _other = other_;
                    }

                    return AssertResultIsOk(arrow::compute::IfElse(_condition, column, _other));
                })};
        }

        return TableOrArray{AssertArrayResultIsOk(
            arrow::compute::IfElse(condition, m_data.second.datum(), other_))};
    }

    auto complete_sort = [](IndexPtr const& index, TableOrArray const& data,
                            arrow::Datum const& key, const auto* options,
                            const std::string& function_name)
    {
        auto sort_indices =
            arrow_utils::call_unary_compute_contiguous_array(key, function_name, options);
        auto sorted_index = arrow_utils::call_compute_contiguous_array(
            {index->array().value(), sort_indices}, "take");
        auto sorted_values = arrow_utils::call_compute_table_or_array(data, {sort_indices}, "take");
        return TableComponent{factory::index::make_index(sorted_index, std::nullopt, index->name()),
                              sorted_values};
    };

    TableComponent Selections::sort_index(bool place_na_last, bool ascending) const
    {
        using namespace arrow::compute;
        arrow::compute::ArraySortOptions options{ascending ? arrow::compute::SortOrder::Ascending
                                                           : arrow::compute::SortOrder::Descending,
                                                 place_na_last
                                                     ? arrow::compute::NullPlacement::AtEnd
                                                     : arrow::compute::NullPlacement::AtStart};
        return complete_sort(m_data.first, m_data.second, m_data.first->array().value(), &options,
                             "array_sort_indices");
    }

    TableComponent Selections::sort_values(std::vector<std::string> const& by, bool place_na_last,
                                           bool ascending) const
    {
        using namespace arrow::compute;
        NullPlacement null_placement =
            place_na_last ? NullPlacement::AtEnd : NullPlacement::AtStart;
        SortOrder order = ascending ? SortOrder::Ascending : SortOrder::Descending;

        auto [index, data] = m_data;
        std::vector<SortKey> sort_keys;
        sort_keys.reserve(by.size());
        for (auto const& key : by)
        {
            sort_keys.emplace_back(key, order);
        }

        SortOptions options{sort_keys, null_placement};
        return complete_sort(m_data.first, m_data.second, m_data.second.datum(), &options,
                             "sort_indices");
    }
} // namespace epoch_frame
