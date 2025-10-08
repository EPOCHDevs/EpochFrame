//
// Created by adesola on 2/13/25.
//

#include "methods_helper.h"
#include "common/epoch_thread_pool.h"
#include "epoch_frame/factory/array_factory.h"
#include "epoch_frame/factory/dataframe_factory.h"
#include "epoch_frame/factory/index_factory.h"
#include "epoch_frame/factory/table_factory.h"
#include "index/arrow_index.h"
#include <arrow/acero/exec_plan.h>
#include <arrow/acero/options.h>
#include <arrow/compute/expression.h>
#include <epoch_core/macros.h>
#include <fmt/format.h>
#include <index/datetime_index.h>
#include <iostream>
#include <tbb/parallel_for.h>
#include <unordered_set>

#include "epoch_frame/common.h"
#include "epoch_frame/dataframe.h"
#include "epoch_frame/frame_or_series.h"
#include "epoch_frame/series.h"
#include "duckdb/c_api_connection.h"

namespace ac = arrow::acero;
namespace cp = arrow::compute;

namespace epoch_frame
{
    arrow::TablePtr add_column(const arrow::TablePtr& table, const std::string& name,
                               const arrow::ChunkedArrayPtr& array)
    {
        auto result =
            table->AddColumn(table->num_columns(), arrow::field(name, array->type()), array);
        AssertFromStream(result.ok(), "Error adding column: " + result.status().ToString());
        return result.MoveValueUnsafe();
    }

    arrow::TablePtr add_column(const arrow::TablePtr& table, const std::string& name,
                               const arrow::ArrayPtr& array)
    {
        return add_column(table, name, factory::array::make_array(array));
    }

    // Consolidated function to compute intersection, union, left_missing, and right_missing
    std::tuple<std::vector<std::string>, std::unordered_set<std::string>>
    compute_sets(const std::vector<std::string>& left_columns,
                 const std::vector<std::string>& right_columns)
    {
        // Make local copies of the vectors to avoid modifying the originals
        std::vector<std::string> left_sorted  = left_columns;
        std::vector<std::string> right_sorted = right_columns;

        // Sort both vectors as set operations require sorted input
        std::sort(left_sorted.begin(), left_sorted.end());
        std::sort(right_sorted.begin(), right_sorted.end());

        // Compute the union
        std::vector<std::string> unions;
        std::ranges::set_union(left_sorted, right_sorted, std::back_inserter(unions));

        std::vector<std::string> intersections;
        std::ranges::set_intersection(left_sorted, right_sorted, std::back_inserter(intersections));

        // Return all results as a tuple
        return std::make_tuple(
            unions, std::unordered_set<std::string>{intersections.begin(), intersections.end()});
    }

    std::pair<IndexPtr, arrow::TablePtr> make_index_table(arrow::TablePtr const& table,
                                                          std::string const&     index_name,
                                                          std::string const&     l_suffix,
                                                          std::string const&     r_suffix)
    {
        const auto left_index_name  = index_name + l_suffix;
        const auto right_index_name = index_name + r_suffix;

        auto l_index = table->GetColumnByName(left_index_name);
        auto r_index = table->GetColumnByName(right_index_name);

        AssertFromStream(l_index != nullptr,
                         "left IIndex column not found: " << left_index_name + l_suffix);
        AssertFromStream(r_index != nullptr,
                         "right IIndex column not found: " << right_index_name + r_suffix);

        auto merged_index = arrow_utils::call_compute_array({l_index, r_index}, "coalesce");

        if (arrow::is_binary_like(merged_index->type()->id()) ||
            arrow::is_nested(merged_index->type()->id()))
        {
            return {factory::index::make_index(merged_index, std::nullopt, index_name), table};
        }

        auto sort_indices = arrow_utils::call_compute_array({merged_index}, "sort_indices");
        auto sorted_index = AssertArrayResultIsOk(arrow::compute::Take(merged_index, sort_indices));
        auto sorted_table = AssertTableResultIsOk(arrow::compute::Take(table, sort_indices));

        return {
            factory::index::make_index(sorted_index, MonotonicDirection::Increasing, index_name),
            sorted_table};
    }

    TableOrArray collect_table_or_array(arrow::TablePtr const&                 table,
                                        arrow::TablePtr const&                 merged_table,
                                        std::vector<std::string> const&        union_columns,
                                        std::unordered_set<std::string> const& intersection_columns,
                                        std::string const&                     suffix,
                                        std::optional<std::string> const&      series_column,
                                        bool                                   broadcast_columns)
    {

        if (series_column)
        {
            return TableOrArray{merged_table->GetColumnByName(*series_column)};
        }

        arrow::ChunkedArrayVector columns;
        arrow::FieldVector        fields;
        columns.reserve(union_columns.size());
        fields.reserve(union_columns.size());

        for (auto const& column_name : union_columns)
        {
            arrow::ChunkedArrayPtr        column;
            std::shared_ptr<arrow::Field> field = table->schema()->GetFieldByName(column_name);
            if (field != nullptr)
            {
                const auto column_with_suffix =
                    intersection_columns.contains(column_name) ? column_name + suffix : column_name;
                column = merged_table->GetColumnByName(column_with_suffix);
                ;
                AssertFromStream(column != nullptr,
                                 "column not found: " << column_with_suffix
                                                      << merged_table->ToString());
            }
            else if (broadcast_columns)
            {
                AssertFalseFromStream(intersection_columns.contains(column_name),
                                      "field not found: " << column_name
                                                          << merged_table->ToString());
                field = merged_table->schema()->GetFieldByName(column_name);
                AssertFromStream(field != nullptr,
                                 "field not found: " << column_name << merged_table->ToString());

                auto array = arrow::MakeArrayOfNull(field->type(), merged_table->num_rows());
                AssertFromStream(array.ok(), array.status().ToString());
                column = factory::array::make_array(array.MoveValueUnsafe());
            }
            else
            {
                continue;
            }
            fields.push_back(field);
            columns.push_back(column);
        }

        return TableOrArray{arrow::Table::Make(arrow::schema(fields), columns)};
    }

    std::tuple<IndexPtr, TableOrArray, TableOrArray>
    align_by_index_and_columns(const TableComponent& left_component,
                               const TableComponent& right_component)
    {
        constexpr auto index_name    = "index";
        constexpr auto left_suffix   = "_l";
        constexpr auto right_suffix  = "_r";
        constexpr auto l_series_name = "left_array";
        constexpr auto r_series_name = "right_array";

        auto left_table  = left_component.second.get_table(l_series_name);
        auto right_table = right_component.second.get_table(r_series_name);

        auto [unions, intersections] =
            compute_sets(left_table->ColumnNames(), right_table->ColumnNames());

        const arrow::TablePtr left_rb =
            add_column(left_table, index_name, left_component.first->array().value());
        const arrow::TablePtr right_rb =
            add_column(right_table, index_name, right_component.first->array().value());

        ac::Declaration left{"table_source", ac::TableSourceNodeOptions(left_rb)};
        ac::Declaration right{"table_source", ac::TableSourceNodeOptions(right_rb)};

        ac::HashJoinNodeOptions join_opts{
            ac::JoinType::FULL_OUTER,
            /*left_keys=*/{index_name},
            /*right_keys=*/{index_name}, cp::literal(true), left_suffix, right_suffix};

        ac::Declaration hashjoin{
            "hashjoin", {std::move(left), std::move(right)}, std::move(join_opts)};

        auto result = ac::DeclarationToTable(hashjoin);
        AssertFromStream(result.ok(), result.status().ToString());

        auto [index, merged] = make_index_table(result.MoveValueUnsafe(), index_name, "_l", "_r");
        bool broadcast_columns =
            left_component.second.is_table() && right_component.second.is_table();
        return std::tuple{
            index,
            collect_table_or_array(left_rb, merged, unions, intersections, left_suffix,
                                   left_component.second.is_chunked_array()
                                       ? std::optional{l_series_name}
                                       : std::nullopt,
                                   broadcast_columns),
            collect_table_or_array(right_rb, merged, unions, intersections, right_suffix,
                                   right_component.second.is_chunked_array()
                                       ? std::optional{r_series_name}
                                       : std::nullopt,
                                   broadcast_columns)};
    }

    TableOrArray align_by_index(const TableComponent& left_table_, const IndexPtr& new_index_,
                                const Scalar& fillValue)
    {
        AssertFromStream(new_index_ != nullptr, "IIndex cannot be null");
        AssertFromStream(left_table_.first != nullptr, "Table IIndex cannot be null");

        if (left_table_.first->equals(new_index_))
        {
            return left_table_.second;
        }

        if (new_index_->size() == 0)
        {
            return factory::table::make_empty_table_or_array(left_table_.second);
        }

        auto left_type  = left_table_.first->dtype();
        auto right_type = new_index_->dtype();
        AssertFromStream(
            left_type->Equals(right_type),
            std::format("IIndex type mismatch. Source index type: {}, Target index type: {}",
                        left_type->ToString(), right_type->ToString()));

        constexpr auto series_name = "series_name";
        if (left_table_.second.get_table(series_name)->num_rows() == 0)
        {
            // Handle the empty source table case
            arrow::TablePtr empty_result;
            auto            schema = left_table_.second.get_table(series_name)->schema();

            // Create a new table with the right number of rows
            if (schema->num_fields() > 0)
            {
                arrow::ChunkedArrayVector arrays;
                for (int i = 0; i < schema->num_fields(); i++)
                {
                    auto field      = schema->field(i);
                    auto null_array = arrow::MakeArrayOfNull(field->type(), new_index_->size());
                    arrays.push_back(
                        arrow::ChunkedArray::Make({null_array.ValueOrDie()}).ValueOrDie());
                }
                empty_result = arrow::Table::Make(schema, arrays);

                if (fillValue.is_valid())
                {
                    empty_result = AssertTableResultIsOk(
                        arrow_utils::call_compute_fill_null_table(empty_result, fillValue.value()));
                }
            }
            else
            {
                // Handle the case of empty schema
                empty_result = factory::table::make_null_table(schema, new_index_->size());
            }

            return factory::table::make_table_or_array(empty_result, series_name);
        }

        const std::string index_name       = "index";
        constexpr auto    left_suffix      = "_l";
        constexpr auto    right_suffix     = "_r";
        auto              left_index_name  = index_name + left_suffix;
        auto              right_index_name = index_name + right_suffix;

        const arrow::TablePtr left_rb     = add_column(left_table_.second.get_table(series_name),
                                                       index_name, left_table_.first->array().value());
        auto                  index_table = new_index_->to_table(index_name);

        ac::Declaration left{"table_source", ac::TableSourceNodeOptions(left_rb)};
        ac::Declaration right{"table_source", ac::TableSourceNodeOptions(index_table)};

        ac::HashJoinNodeOptions join_opts{
            ac::JoinType::RIGHT_OUTER,
            /*left_keys=*/{index_name},
            /*right_keys=*/{index_name}, cp::literal(true), left_suffix, right_suffix};

        ac::Declaration hashjoin{
            "hashjoin", {std::move(left), std::move(right)}, std::move(join_opts)};

        // cp::Expression  filter_expr = cp::is_valid(cp::field_ref(right_index_name));
        // ac::Declaration filter{
        //     "filter", {std::move(hashjoin)}, ac::FilterNodeOptions(std::move(filter_expr))};

        // arrow::TablePtr merged = AssertResultIsOk(ac::DeclarationToTable(filter));
        arrow::TablePtr merged = AssertResultIsOk(ac::DeclarationToTable(hashjoin));

        auto right_index_pos = merged->schema()->GetFieldIndex(right_index_name);
        auto left_index_pos  = merged->schema()->GetFieldIndex(left_index_name);

        AssertFromStream(left_index_pos != -1, "Failed to find left index after alignment merge.");
        AssertFromStream(right_index_pos != -1,
                         "Failed to find right index after alignment merge.");

        auto sorted_table = AssertTableResultIsOk(cp::Take(
            merged, AssertResultIsOk(SortIndices(merged, arrow::SortOptions({cp::SortKey{
                                                             arrow::FieldRef{right_index_pos},
                                                         }})))));

        auto new_table =
            AssertResultIsOk(AssertResultIsOk(sorted_table->RemoveColumn(right_index_pos))
                                 ->RemoveColumn(left_index_pos));
        if (new_table->schema()->num_fields() == 0 || new_table->num_rows() == 0)
        {
            new_table = factory::table::make_null_table(new_table->schema(), new_index_->size());
        }

        if (fillValue.is_valid())
        {
            new_table = AssertTableResultIsOk(
                arrow_utils::call_compute_fill_null_table(new_table, fillValue.value()));
        }

        AssertFromStream(new_table->num_rows() == static_cast<int64_t>(new_index_->size()),
                         "Alignment error: Result size (" << new_table->num_rows()
                                                          << ") doesn't match index size ("
                                                          << new_index_->size() << ")");
        return factory::table::make_table_or_array(new_table, series_name);
    }

    std::pair<bool, IndexPtr> combine_index(std::vector<FrameOrSeries> const& objs, bool intersect)
    {
        bool index_all_equal = true;
        auto merged_index    = std::accumulate(
            objs.begin() + 1, objs.end(), objs.at(0).index(),
            [&](const IndexPtr& acc, FrameOrSeries const& obj)
            {
                auto next_index = obj.index();
                AssertFromStream(acc->dtype()->Equals(next_index->dtype()),
                                    "concat multiple frames requires same index");
                auto index_equal = acc->equals(next_index);
                if (index_equal)
                {
                    return acc;
                }
                index_all_equal = false;
                return intersect ? acc->intersection(next_index) : acc->union_(next_index);
            });
        return {index_all_equal, merged_index};
    }

    std::vector<FrameOrSeries> remove_empty_objs(std::vector<FrameOrSeries> const& objs)
    {
        std::vector<FrameOrSeries> cleanedObjs;
        cleanedObjs.reserve(objs.size());
        std::ranges::copy_if(objs, std::back_inserter(cleanedObjs),
                             [](FrameOrSeries const& obj) { return (obj.size() > 0); });
        return cleanedObjs;
    }

    DataFrame concat_column_unsafe(std::vector<FrameOrSeries> const& objs, IndexPtr const& newIndex,
                                   bool ignore_index)
    {
        arrow::ChunkedArrayVector       arrays;
        arrow::FieldVector              fields;
        int64_t                         i = 0;
        std::unordered_set<std::string> names;
        for (auto const& obj : objs)
        {
            auto [columns_, fields_] = obj.visit(
                [&]<typename T>(const T& variant_)
                {
                    if constexpr (std::same_as<T, DataFrame>)
                    {
                        return std::pair<arrow::ChunkedArrayVector, arrow::FieldVector>{
                            variant_.table()->columns(), variant_.table()->schema()->fields()};
                    }
                    else
                    {
                        auto n = variant_.name();
                        if (!n)
                        {
                            n = std::to_string(i++);
                        }
                        else if (names.contains(n.value()))
                        {
                            n = n.value() + "_" + std::to_string(i++);
                        }
                        names.insert(n.value());
                        return std::pair<arrow::ChunkedArrayVector, arrow::FieldVector>{
                            std::vector{variant_.array()},
                            {arrow::field(n.value(), variant_.array()->type())}};
                    }
                });
            arrays.insert(arrays.end(), columns_.begin(), columns_.end());
            fields.insert(fields.end(), fields_.begin(), fields_.end());
        }

        // Check for duplicate column names before creating DataFrame
        std::unordered_set<std::string> unique_names;
        std::vector<std::string>        duplicates;
        for (const auto& field : fields)
        {
            const auto& name = field->name();
            if (unique_names.contains(name))
            {
                duplicates.push_back(name);
            }
            else
            {
                unique_names.insert(name);
            }
        }

        if (!duplicates.empty())
        {
            std::string duplicate_list;
            for (size_t i = 0; i < duplicates.size(); ++i)
            {
                if (i > 0)
                    duplicate_list += ", ";
                duplicate_list += "'" + duplicates[i] + "'";
            }
            throw std::runtime_error(fmt::format(
                "concat: Duplicate column names detected: {}. "
                "Use different column names or consider using suffixes to avoid conflicts.",
                duplicate_list));
        }
        auto new_table = arrow::Table::Make(arrow::schema(fields), arrays);
        return ignore_index ? DataFrame(new_table) : DataFrame(newIndex, new_table);
    }

    DataFrame concat_column_safe(std::vector<FrameOrSeries> const& objs, IndexPtr const& newIndex,
                                 bool ignore_index)
    {
        std::vector<FrameOrSeries> aligned_frames(objs.size());

        EpochThreadPool::getInstance().execute(
            [&]
            {
                tbb::parallel_for(
                    tbb::blocked_range<size_t>(0UL, objs.size()),
                    [&](const tbb::blocked_range<size_t>& r)
                    {
                        for (size_t i = r.begin(); i != r.end(); ++i)
                        {
                            auto obj         = objs[i];
                            auto table_array = align_by_index(
                                TableComponent{obj.index(), obj.table_or_array()}, newIndex);
                            if (table_array.is_table())
                            {
                                aligned_frames[i] = FrameOrSeries(newIndex, table_array.table());
                            }
                            else
                            {
                                auto name = obj.series().name();
                                aligned_frames[i] =
                                    FrameOrSeries(newIndex, table_array.chunked_array(), name);
                            }
                        }
                    },
                    tbb::simple_partitioner());
            });
        return concat_column_unsafe(aligned_frames, newIndex, ignore_index);
    }

    DataFrame concat_row(std::vector<FrameOrSeries> const& objs, bool ignore_index, bool intersect)
    {
        std::vector<arrow::TablePtr> tables(objs.size());
        EpochThreadPool::getInstance().execute(
            [&]
            {
                tbb::parallel_for(
                    tbb::blocked_range<size_t>(0UL, objs.size()),
                    [&](const tbb::blocked_range<size_t>& r)
                    {
                        for (size_t i = r.begin(); i != r.end(); ++i)
                        {
                            tables[i] = objs[i].table();
                        }
                    },
                    tbb::simple_partitioner());
            });

        // Generate unique index column name to avoid collisions
        const std::string indexName = ignore_index ? "__index__" : get_unique_index_column_name(tables);

        // Add index columns after determining unique name
        if (!ignore_index)
        {
            EpochThreadPool::getInstance().execute(
                [&]
                {
                    tbb::parallel_for(
                        tbb::blocked_range<size_t>(0UL, objs.size()),
                        [&](const tbb::blocked_range<size_t>& r)
                        {
                            for (size_t i = r.begin(); i != r.end(); ++i)
                            {
                                tables[i] = add_column(tables[i], indexName,
                                                       objs[i].index()->array().value());
                            }
                        },
                        tbb::simple_partitioner());
                });
        }

        arrow::ConcatenateTablesOptions options;
        options.field_merge_options = arrow::Field::MergeOptions::Permissive();
        options.unify_schemas       = intersect;

        arrow::TablePtr merged = AssertResultIsOk(arrow::ConcatenateTables(tables));
        if (ignore_index)
        {
            return DataFrame{merged};
        }

        auto indexField = merged->schema()->GetFieldIndex(indexName);
        AssertFromStream(indexField != -1, "Failed to find index");
        auto index = merged->column(indexField);

        if (index->type()->id() == arrow::Type::TIMESTAMP)
        {
            const auto sorted_index = AssertResultIsOk(
                SortIndices(merged, arrow::SortOptions{{arrow::compute::SortKey{indexName}}}));
            merged = AssertTableResultIsOk(arrow::compute::Take(merged, sorted_index));
            Array dt_index(factory::array::make_contiguous_array(merged->column(indexField)));
            return DataFrame(std::make_shared<DateTimeIndex>(dt_index.value(), ""),
                             AssertResultIsOk(merged->RemoveColumn(indexField)));
        }

        Array dt_index(factory::array::make_contiguous_array(merged->column(indexField)));
        return DataFrame(factory::index::make_index(index, std::nullopt, ""),
                         AssertResultIsOk(merged->RemoveColumn(indexField)));
    }

    // ============================================================================
    // Helper functions for concat - Following Single Responsibility Principle
    // ============================================================================

    struct ConcatInputs
    {
        std::vector<DataFrame> dataframes;
        std::vector<IndexPtr> indices;
        std::vector<arrow::TablePtr> tables;
    };

    ConcatInputs prepare_concat_inputs(std::vector<FrameOrSeries> const& objs)
    {
        ConcatInputs result;
        result.dataframes.reserve(objs.size());
        result.indices.reserve(objs.size());
        result.tables.reserve(objs.size());

        for (const auto& obj : objs) {
            DataFrame df = obj.to_frame();
            result.dataframes.push_back(df);
            result.indices.push_back(df.index());
            result.tables.push_back(df.table());
        }

        return result;
    }

    arrow::SchemaPtr build_unified_schema(
        std::vector<arrow::TablePtr> const& tables,
        std::vector<arrow::TablePtr> const& tables_with_index,
        std::vector<IndexPtr> const& indices,
        bool ignore_index,
        std::string const& index_name)
    {
        // Collect all unique columns across all tables
        std::set<std::string> all_columns;
        for (const auto& table : tables) {
            auto col_names = table->ColumnNames();
            all_columns.insert(col_names.begin(), col_names.end());
        }

        // Build unified schema with all columns
        arrow::FieldVector unified_fields;
        if (!ignore_index) {
            unified_fields.push_back(arrow::field(index_name, indices[0]->dtype()));
        }

        // Collect field types from tables
        std::map<std::string, std::shared_ptr<arrow::DataType>> column_types;
        for (const auto& table : tables_with_index) {
            for (int i = 0; i < table->num_columns(); i++) {
                auto field = table->schema()->field(i);
                if (field->name() != index_name || ignore_index) {
                    column_types[field->name()] = field->type();
                }
            }
        }

        for (const auto& col : all_columns) {
            if (column_types.contains(col)) {
                unified_fields.push_back(arrow::field(col, column_types[col]));
            }
        }

        return arrow::schema(unified_fields);
    }

    std::vector<arrow::TablePtr> align_tables_to_schema(
        std::vector<arrow::TablePtr> const& tables_with_index,
        arrow::SchemaPtr const& unified_schema)
    {
        std::vector<arrow::TablePtr> aligned_tables(tables_with_index.size());

        EpochThreadPool::getInstance().execute(
            [&]
            {
                tbb::parallel_for(
                    tbb::blocked_range<size_t>(0UL, tables_with_index.size()),
                    [&](const tbb::blocked_range<size_t>& r)
                    {
                        for (size_t i = r.begin(); i != r.end(); ++i)
                        {
                            auto table = tables_with_index[i];
                            arrow::ChunkedArrayVector columns;

                            for (const auto& field : unified_schema->fields()) {
                                auto existing_col = table->GetColumnByName(field->name());
                                if (existing_col) {
                                    columns.push_back(existing_col);
                                } else {
                                    // Add null column
                                    auto null_array = AssertResultIsOk(
                                        arrow::MakeArrayOfNull(field->type(), table->num_rows()));
                                    columns.push_back(factory::array::make_array(null_array));
                                }
                            }

                            aligned_tables[i] = arrow::Table::Make(unified_schema, columns);
                        }
                    },
                    tbb::simple_partitioner());
            });

        return aligned_tables;
    }

    DataFrame extract_index_from_merged_table(
        arrow::TablePtr merged,
        std::string const& index_name,
        bool ignore_index)
    {
        if (ignore_index)
        {
            return DataFrame{merged};
        }

        auto indexField = merged->schema()->GetFieldIndex(index_name);
        AssertFromStream(indexField != -1, "Failed to find index");
        auto index = merged->column(indexField);

        if (index->type()->id() == arrow::Type::TIMESTAMP)
        {
            const auto sorted_index = AssertResultIsOk(
                SortIndices(merged, arrow::SortOptions{{arrow::compute::SortKey{index_name}}}));
            merged = AssertTableResultIsOk(arrow::compute::Take(merged, sorted_index));
            Array dt_index(factory::array::make_contiguous_array(merged->column(indexField)));
            return DataFrame(std::make_shared<DateTimeIndex>(dt_index.value(), ""),
                             AssertResultIsOk(merged->RemoveColumn(indexField)));
        }
        else
        {
            Array dt_index(factory::array::make_contiguous_array(merged->column(indexField)));
            return DataFrame(factory::index::make_index(index, std::nullopt, ""),
                             AssertResultIsOk(merged->RemoveColumn(indexField)));
        }
    }

    std::vector<std::string> check_duplicate_columns(std::vector<arrow::TablePtr> const& tables)
    {
        std::map<std::string, int> column_name_counts;
        for (const auto& table : tables) {
            for (const auto& col_name : table->ColumnNames()) {
                column_name_counts[col_name]++;
            }
        }

        std::vector<std::string> duplicate_columns;
        for (const auto& [col_name, count] : column_name_counts) {
            if (count > 1) {
                duplicate_columns.push_back(col_name);
            }
        }

        return duplicate_columns;
    }

    std::string get_unique_index_column_name(std::vector<arrow::TablePtr> const& tables)
    {
        // Collect all existing column names from all tables
        std::unordered_set<std::string> existing_columns;
        for (const auto& table : tables) {
            auto col_names = table->ColumnNames();
            existing_columns.insert(col_names.begin(), col_names.end());
        }

        // Try the default name first
        std::string candidate = "__index__";
        if (!existing_columns.contains(candidate)) {
            return candidate;
        }

        // If __index__ exists, try __index_0__, __index_1__, etc.
        int suffix = 0;
        while (true) {
            candidate = "__index_" + std::to_string(suffix) + "__";
            if (!existing_columns.contains(candidate)) {
                return candidate;
            }
            suffix++;
            // Safety check to prevent infinite loop
            AssertFromStream(suffix < 1000,
                "Unable to find unique index column name after 1000 attempts");
        }
    }

    bool check_index_overlap(std::vector<IndexPtr> const& indices)
    {
        // Use existing IIndex::intersection() method - handles all Arrow types correctly
        // This is 10-100x faster than manual loops and supports TIMESTAMP, STRING, etc.
        auto intersection = indices[0];
        for (size_t i = 1; i < indices.size(); i++) {
            intersection = intersection->intersection(indices[i]);
            if (intersection->size() == 0) {
                return false;  // No overlap
            }
        }
        return true;  // Has overlap
    }

    ac::Declaration build_acero_join_plan(
        std::vector<arrow::TablePtr> const& tables_with_index,
        ac::JoinType join_type,
        std::string const& index_name)
    {
        // For single table, just return it
        if (tables_with_index.size() == 1) {
            return ac::Declaration{"table_source", ac::TableSourceNodeOptions(tables_with_index[0])};
        }

        // For 2 tables, use simple join
        if (tables_with_index.size() == 2) {
            ac::Declaration left{"table_source", ac::TableSourceNodeOptions(tables_with_index[0])};
            ac::Declaration right{"table_source", ac::TableSourceNodeOptions(tables_with_index[1])};

            ac::HashJoinNodeOptions join_opts{
                join_type,
                /*left_keys=*/{index_name},
                /*right_keys=*/{index_name},
                cp::literal(true),
                "_left_1",
                "_right_1"
            };

            return ac::Declaration{
                "hashjoin",
                {std::move(left), std::move(right)},
                std::move(join_opts)
            };
        }

        // For 3+ tables, we need to materialize and re-add index column after each join
        // to avoid Acero's "No match or multiple matches" error
        arrow::TablePtr current_table = tables_with_index[0];

        for (size_t i = 1; i < tables_with_index.size(); i++) {
            ac::Declaration left{"table_source", ac::TableSourceNodeOptions(current_table)};
            ac::Declaration right{"table_source", ac::TableSourceNodeOptions(tables_with_index[i])};

            std::string left_suffix = "_left_" + std::to_string(i);
            std::string right_suffix = "_right_" + std::to_string(i);

            ac::HashJoinNodeOptions join_opts{
                join_type,
                /*left_keys=*/{index_name},
                /*right_keys=*/{index_name},
                cp::literal(true),
                left_suffix,
                right_suffix
            };

            ac::Declaration hashjoin{
                "hashjoin",
                {std::move(left), std::move(right)},
                std::move(join_opts)
            };

            // Materialize the join result
            current_table = AssertResultIsOk(ac::DeclarationToTable(hashjoin));

            // Coalesce the index columns and re-add with original name
            auto index_col_left = current_table->GetColumnByName(index_name + left_suffix);
            auto index_col_right = current_table->GetColumnByName(index_name + right_suffix);

            if (index_col_left && index_col_right) {
                auto coalesced = arrow_utils::call_compute_array({index_col_left, index_col_right}, "coalesce");

                // Remove the suffixed index columns
                auto left_idx = current_table->schema()->GetFieldIndex(index_name + left_suffix);
                auto right_idx = current_table->schema()->GetFieldIndex(index_name + right_suffix);

                if (left_idx >= 0) {
                    current_table = AssertResultIsOk(current_table->RemoveColumn(left_idx));
                    // After removing left, right index shifts down
                    right_idx = current_table->schema()->GetFieldIndex(index_name + right_suffix);
                }
                if (right_idx >= 0) {
                    current_table = AssertResultIsOk(current_table->RemoveColumn(right_idx));
                }

                // Add coalesced index back with original name
                current_table = AssertResultIsOk(
                    current_table->AddColumn(0, arrow::field(index_name, coalesced->type()), coalesced));
            }
        }

        return ac::Declaration{"table_source", ac::TableSourceNodeOptions(current_table)};
    }

    arrow::ChunkedArrayPtr coalesce_index_columns(
        arrow::TablePtr const& merged,
        std::string const& index_name)
    {
        // Find all index columns (with suffixes from joins)
        std::vector<std::string> index_columns;
        for (int i = 0; i < merged->num_columns(); i++) {
            auto col_name = merged->schema()->field(i)->name();
            if (col_name == index_name ||
                col_name.find(std::string(index_name) + "_left_") == 0 ||
                col_name.find(std::string(index_name) + "_right_") == 0) {
                index_columns.push_back(col_name);
            }
        }

        if (index_columns.empty()) {
            return nullptr;
        }

        // Gather index arrays
        std::vector<arrow::ChunkedArrayPtr> index_arrays;
        for (const auto& col_name : index_columns) {
            index_arrays.push_back(merged->GetColumnByName(col_name));
        }

        if (index_arrays.size() == 1) {
            return index_arrays[0];
        }

        // Coalesce multiple index columns
        std::vector<arrow::Datum> datums;
        for (const auto& arr : index_arrays) {
            datums.push_back(arr);
        }
        return arrow_utils::call_compute_array(datums, "coalesce");
    }

    arrow::TablePtr remove_index_columns(
        arrow::TablePtr merged,
        std::string const& index_name)
    {
        // Find and remove all index-related columns
        std::vector<std::string> columns_to_remove;
        for (int i = 0; i < merged->num_columns(); i++) {
            auto col_name = merged->schema()->field(i)->name();
            if (col_name == index_name ||
                col_name.find(std::string(index_name) + "_left_") == 0 ||
                col_name.find(std::string(index_name) + "_right_") == 0) {
                columns_to_remove.push_back(col_name);
            }
        }

        for (const auto& col_name : columns_to_remove) {
            auto col_idx = merged->schema()->GetFieldIndex(col_name);
            if (col_idx >= 0) {
                merged = AssertResultIsOk(merged->RemoveColumn(col_idx));
            }
        }

        return merged;
    }

    // Main row concatenation function using Acero
    DataFrame concat_rows_acero(
        std::vector<arrow::TablePtr> const& tables,
        std::vector<IndexPtr> const& indices,
        bool ignore_index)
    {
        // Generate unique index column name to avoid collisions with existing columns
        const std::string indexName = ignore_index ? "__index__" : get_unique_index_column_name(tables);

        // Add index columns in parallel
        std::vector<arrow::TablePtr> tables_with_index(tables.size());
        EpochThreadPool::getInstance().execute(
            [&]
            {
                tbb::parallel_for(
                    tbb::blocked_range<size_t>(0UL, tables.size()),
                    [&](const tbb::blocked_range<size_t>& r)
                    {
                        for (size_t i = r.begin(); i != r.end(); ++i)
                        {
                            tables_with_index[i] = tables[i];
                            if (!ignore_index)
                            {
                                tables_with_index[i] = add_column(tables_with_index[i], indexName,
                                                       indices[i]->array().value());
                            }
                        }
                    },
                    tbb::simple_partitioner());
            });

        // Build unified schema and align tables
        auto unified_schema = build_unified_schema(tables, tables_with_index, indices, ignore_index, indexName);
        auto aligned_tables = align_tables_to_schema(tables_with_index, unified_schema);

        // Concatenate aligned tables
        arrow::TablePtr merged = AssertResultIsOk(arrow::ConcatenateTables(aligned_tables));

        // Extract and reconstruct index
        return extract_index_from_merged_table(merged, indexName, ignore_index);
    }

    // Main column concatenation function using Acero
    DataFrame concat_columns_acero(
        std::vector<arrow::TablePtr> const& tables,
        std::vector<IndexPtr> const& indices,
        JoinType join_type,
        bool ignore_index)
    {
        // Generate unique index column name to avoid collisions with existing columns
        // This is critical: if any input table has a column named "__index__",
        // Acero's join will fail with "No match or multiple matches for key field reference"
        const std::string indexName = get_unique_index_column_name(tables);

        // Check for duplicate column names
        auto duplicate_columns = check_duplicate_columns(tables);
        bool has_duplicates = !duplicate_columns.empty();

        // For inner join with duplicates, check if indices overlap
        if (has_duplicates && join_type == JoinType::Inner) {
            if (!check_index_overlap(indices)) {
                // No overlapping indices - return empty DataFrame
                return make_empty_dataframe(indices[0]->dtype());
            }
        }

        // If we have duplicates and would have rows, throw error
        if (has_duplicates) {
            std::string duplicate_list;
            for (size_t i = 0; i < duplicate_columns.size(); ++i) {
                if (i > 0) duplicate_list += ", ";
                duplicate_list += "'" + duplicate_columns[i] + "'";
            }
            throw std::runtime_error(fmt::format(
                "concat: Duplicate column names detected: {}. "
                "Use different column names or consider using suffixes to avoid conflicts.",
                duplicate_list));
        }

        // Add index as column to all tables
        std::vector<arrow::TablePtr> tables_with_index(tables.size());
        for (size_t i = 0; i < tables.size(); i++) {
            auto index_array = indices[i]->array().as_chunked_array();
            auto index_field = arrow::field(indexName, index_array->type());
            tables_with_index[i] = AssertResultIsOk(
                tables[i]->AddColumn(0, index_field, index_array));
        }

        // Build and execute join plan
        ac::JoinType acero_join_type = (join_type == JoinType::Inner)
                                ? ac::JoinType::INNER
                                : ac::JoinType::FULL_OUTER;
        auto join_plan = build_acero_join_plan(tables_with_index, acero_join_type, indexName);
        arrow::TablePtr merged = AssertResultIsOk(ac::DeclarationToTable(join_plan));

        // Coalesce and sort by index
        auto final_index_array = coalesce_index_columns(merged, indexName);
        if (final_index_array) {
            auto sort_indices = arrow_utils::call_compute_array({final_index_array}, "sort_indices");
            merged = AssertTableResultIsOk(arrow::compute::Take(merged, sort_indices));
            final_index_array = AssertArrayResultIsOk(
                arrow::compute::Take(final_index_array, sort_indices));
        }

        // Remove index columns
        merged = remove_index_columns(merged, indexName);

        // Create final DataFrame with reconstructed index
        if (!ignore_index && final_index_array) {
            auto final_index = factory::index::make_index(
                factory::array::make_contiguous_array(final_index_array),
                std::nullopt,
                "");
            return DataFrame(final_index, merged);
        } else {
            return DataFrame(factory::index::from_range(merged->num_rows()), merged);
        }
    }

    DataFrame concat(const ConcatOptions& options)
    {
        // Validate input
        if (options.frames.empty())
        {
            throw std::runtime_error("concat: no frames to concatenate");
        }

        // Early exit for single frame
        if (options.frames.size() == 1)
        {
            return options.frames.front().to_frame();
        }

        // Remove empty objects
        std::vector<FrameOrSeries> cleanedObjs = remove_empty_objs(options.frames);
        if (cleanedObjs.empty())
        {
            return make_empty_dataframe(options.frames.front().index()->dtype());
        }
        if (cleanedObjs.size() == 1)
        {
            return (options.joinType == JoinType::Inner)
                       ? make_empty_dataframe(options.frames.front().index()->dtype())
                       : cleanedObjs.front().to_frame();
        }

        // Prepare inputs - convert FrameOrSeries to working data structures
        auto [dataframes, indices, tables] = prepare_concat_inputs(cleanedObjs);

        // Dispatch to appropriate concat function
        DataFrame frame = (options.axis == AxisType::Row)
            ? concat_rows_acero(tables, indices, options.ignore_index)
            : concat_columns_acero(tables, indices, options.joinType, options.ignore_index);

        // Apply sorting if requested
        if (options.sort)
        {
            return options.axis == AxisType::Column ? frame.sort_index() : frame.sort_columns();
        }
        return frame;
    }

    DataFrame merge(const MergeOptions& options)
    {
        // Convert to DataFrames
        DataFrame df_left  = options.left.to_frame();
        DataFrame df_right = options.right.to_frame();

        // Prepare tables and indices for Acero
        std::vector<arrow::TablePtr> tables = {df_left.table(), df_right.table()};
        std::vector<IndexPtr> indices = {df_left.index(), df_right.index()};

        // Dispatch to concat_*_acero functions (they handle N tables, 2 is trivial)
        DataFrame frame = (options.axis == AxisType::Row)
            ? concat_rows_acero(tables, indices, options.ignore_index)
            : concat_columns_acero(tables, indices, options.joinType, options.ignore_index);

        // Apply sorting if requested
        if (options.sort) {
            return options.axis == AxisType::Column ? frame.sort_index() : frame.sort_columns();
        }

        return frame;
    }

    //----------------------------------------------------------------------
    // Helper: binary_op_aligned(left, right, op)
    //
    // Both left/right must have the same number of rows and share a row-aligned
    // index column. We do column-by-column arithmetic for columns with the same name.
    //
    // *If you want behavior exactly like Pandas df+df = union of columns with nulls
    //  for missing combos, then you'd do additional "coalesce" steps. Here we do
    //  the simplest approach: only apply the arithmetic to columns found in both.*
    //----------------------------------------------------------------------
    TableOrArray unsafe_binary_op(const TableOrArray& left_rb, const TableOrArray& right_rb,
                                  const std::string& op)
    {
        auto const [left, right] = std::visit(
            []<typename T1, typename T2>(
                T1 const& lhs, T2 const& rhs) -> std::tuple<arrow::TablePtr, arrow::TablePtr>
            {
                if constexpr (std::same_as<T1, arrow::Datum::Empty> ||
                              std::same_as<T2, arrow::Datum::Empty>)
                {
                    throw std::runtime_error("Unsupported unsafe_binary_op type");
                }
                else
                {
                    using U1 = typename T1::element_type;
                    using U2 = typename T2::element_type;
                    if constexpr (std::is_same_v<U1, arrow::Table> &&
                                  std::is_same_v<U2, arrow::Table>)
                    {
                        return std::tuple(lhs, rhs);
                    }
                    if constexpr (std::is_same_v<U1, arrow::Table> &&
                                  std::is_same_v<U2, arrow::ChunkedArray>)
                    {
                        return std::tuple(lhs, make_table_from_array_schema(*lhs, rhs));
                    }
                    else if constexpr (std::is_same_v<U1, arrow::ChunkedArray> &&
                                       std::is_same_v<U2, arrow::Table>)
                    {
                        return std::tuple(make_table_from_array_schema(*rhs, lhs), rhs);
                    }
                    else if constexpr (std::is_same_v<U1, arrow::ChunkedArray> &&
                                       std::is_same_v<U2, arrow::ChunkedArray>)
                    {
                        return std::tuple(nullptr, nullptr);
                    }
                    else
                    {
                        throw std::runtime_error("Unsupported unsafe_binary_op type");
                    }
                }
            },
            left_rb.datum().value, right_rb.datum().value);

        if (left == nullptr && right == nullptr)
        {
            return TableOrArray{
                arrow_utils::call_compute_array({left_rb.datum(), right_rb.datum()}, op)};
        }

        return TableOrArray{arrow_utils::apply_function_to_table(
            left,
            [&](arrow::Datum const& lhs, std::string const& column_name)
            {
                const auto& rhs = right->GetColumnByName(column_name);
                AssertFromStream(rhs != nullptr, "column not found: " << column_name);
                return arrow::Datum(arrow_utils::call_compute_array({lhs, rhs}, op));
            })};
    }

    bool has_unique_type(const arrow::SchemaPtr& schema)
    {
        if (schema->num_fields() == 0)
        {
            return false;
        }
        auto type = schema->field(0)->type();
        return std::ranges::all_of(schema->fields(),
                                   [type](const auto& field) { return field->type() == type; });
    }

    DictionaryEncodeResult dictionary_encode(const arrow::ArrayPtr& array)
    {
        auto dict_res =
            arrow::compute::DictionaryEncode(array, arrow::compute::DictionaryEncodeOptions{});
        ;
        if (!dict_res.ok())
        {
            throw std::runtime_error(dict_res.status().ToString());
        }

        auto dict_array = std::dynamic_pointer_cast<arrow::DictionaryArray>(dict_res->make_array());
        if (!dict_array)
        {
            throw std::runtime_error(
                "duplicated(): dictionary_encode didn't return DictionaryArray");
        }

        auto codes = std::dynamic_pointer_cast<arrow::Int32Array>(dict_array->indices());
        return {codes, dict_array->dictionary()};
    }

    ValueCountResult value_counts(const arrow::ArrayPtr& array)
    {
        // call the Arrow "value_counts" kernel
        auto struct_arr = AssertResultIsOk(arrow::compute::ValueCounts(array));

        // The struct has 2 fields: "values" (same type as m_array) and "counts" (Int64)
        auto values_arr = struct_arr->GetFieldByName("values");
        auto counts_arr =
            std::dynamic_pointer_cast<arrow::Int64Array>(struct_arr->GetFieldByName("counts"));
        if (!counts_arr)
        {
            throw std::runtime_error("value_counts: 'counts' field is not Int64Array");
        }
        return {counts_arr, values_arr};
    }

    arrow::ChunkedArrayPtr get_column_by_name(arrow::Table const& table, std::string const& name)
    {
        auto column = table.GetColumnByName(name);
        AssertFromStream(column != nullptr, "ColumnNotFound: " << name);
        return column;
    }

    arrow::FieldPtr get_field_by_name(arrow::Schema const& schema, std::string const& name)
    {
        auto field = schema.GetFieldByName(name);
        AssertFromStream(field != nullptr, "FieldNotFound: " << name);
        return field;
    }

    arrow::SchemaPtr slice_schema(arrow::Schema const&            schema,
                                  std::vector<std::string> const& column_names)
    {
        arrow::FieldVector fields(column_names.size());
        std::ranges::transform(column_names, fields.begin(),
                               [&](auto const& name) { return get_field_by_name(schema, name); });
        return arrow::schema(fields);
    }

    DataFrame get_variant_column(DataFrame const& frame, const LocColArgumentVariant& colVariant)
    {
        return std::visit([&](auto const& variant_type) { return frame[variant_type]; },
                          colVariant);
    }

    DataFrame get_variant_row(DataFrame const& frame, const LocRowArgumentVariant& rowVariant)
    {
        return std::visit([&](auto const& variant_type) { return frame.loc(variant_type); },
                          rowVariant);
    }

    Series get_variant_row(Series const& series, const LocRowArgumentVariant& rowVariant)
    {
        return std::visit(
            [&]<typename T>(T const& variant_type) -> Series
            {
                if constexpr (std::is_same_v<T, DataFrameToSeriesCallable>)
                {
                    throw std::runtime_error(
                        "DataFrameToSeriesCallable is not supported for Series loc");
                }
                else
                {
                    return series.loc(variant_type);
                }
            },
            rowVariant);
    }

    arrow::TablePtr make_table_from_array_schema(arrow::Table const&           table,
                                                 arrow::ChunkedArrayPtr const& array)
    {
        return arrow::Table::Make(table.schema(),
                                  std::vector{static_cast<size_t>(table.num_columns()), array});
    }

    arrow::ChunkedArrayPtr get_array(arrow::Table const& table, std::string const& name,
                                     arrow::Scalar const& default_value)
    {
        auto column = table.GetColumnByName(name);
        if (column)
        {
            return column;
        }
        return AssertArrayResultIsOk(arrow::MakeArrayFromScalar(default_value, table.num_rows()));
    }
} // namespace epoch_frame
