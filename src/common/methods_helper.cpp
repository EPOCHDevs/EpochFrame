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
        constexpr const char* indexName = "__index__";

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
                            if (!ignore_index)
                            {
                                tables[i] = add_column(tables[i], indexName,
                                                       objs[i].index()->array().value());
                            }
                        }
                    },
                    tbb::simple_partitioner());
            });

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

    DataFrame concat(const ConcatOptions& options)
    {
        DataFrame frame;
        if (options.frames.empty())
        {
            throw std::runtime_error("concat: no frames to concatenate");
        }

        if (options.frames.size() == 1)
        {
            return options.frames.front().to_frame();
        }

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

        // Use DuckDB for concatenation
        auto conn = std::make_unique<CAPIConnection>();

        // Register all DataFrames/Series as DuckDB tables
        std::vector<std::string> table_names;
        std::vector<IndexPtr> indices;
        std::vector<std::vector<std::string>> column_names_list;

        for (size_t i = 0; i < cleanedObjs.size(); i++) {
            std::string table_name = "t" + std::to_string(i);
            table_names.push_back(table_name);

            DataFrame df = cleanedObjs[i].to_frame();
            indices.push_back(df.index());
            column_names_list.push_back(df.column_names());

            // Add index as a column for join operations
            auto table_with_index = df.table();
            if (!options.ignore_index && options.axis == AxisType::Column) {
                auto index_array = df.index()->array().as_chunked_array();
                auto index_field = arrow::field("__index__", index_array->type());
                table_with_index = AssertResultIsOk(
                    table_with_index->AddColumn(0, index_field, index_array));
            }

            conn->registerArrowTable(table_name, table_with_index);
        }

        std::string sql;
        std::shared_ptr<arrow::Table> result_table;

        if (options.axis == AxisType::Row) {
            // Row concatenation using UNION ALL
            // Build column list for consistent schema
            std::set<std::string> all_columns;
            for (const auto& col_list : column_names_list) {
                all_columns.insert(col_list.begin(), col_list.end());
            }

            // Handle case of no columns (empty dataframes)
            if (all_columns.empty() || (all_columns.size() == 1 && all_columns.begin()->empty())) {
                sql = "SELECT * FROM " + table_names[0];
                for (size_t i = 1; i < table_names.size(); i++) {
                    sql += " UNION ALL SELECT * FROM " + table_names[i];
                }
            } else {
                // Remove empty strings from all_columns
                all_columns.erase("");

                if (all_columns.empty()) {
                    // If all columns were empty strings
                    sql = "SELECT * FROM " + table_names[0];
                    for (size_t i = 1; i < table_names.size(); i++) {
                        sql += " UNION ALL SELECT * FROM " + table_names[i];
                    }
                } else {
                    sql = "SELECT ";
                    bool first_col = true;
                    for (const auto& col : all_columns) {
                        if (!first_col) sql += ", ";

                        // Check if column exists in first table
                        if (std::find(column_names_list[0].begin(), column_names_list[0].end(), col)
                            != column_names_list[0].end()) {
                            sql += "\"" + col + "\"";
                        } else {
                            sql += "NULL AS \"" + col + "\"";
                        }
                        first_col = false;
                    }
                    sql += " FROM " + table_names[0];

                    for (size_t i = 1; i < table_names.size(); i++) {
                        sql += " UNION ALL SELECT ";
                        first_col = true;
                        for (const auto& col : all_columns) {
                            if (!first_col) sql += ", ";

                            if (std::find(column_names_list[i].begin(), column_names_list[i].end(), col)
                                != column_names_list[i].end()) {
                                sql += "\"" + col + "\"";
                            } else {
                                sql += "NULL AS \"" + col + "\"";
                            }
                            first_col = false;
                        }
                        sql += " FROM " + table_names[i];
                    }
                }
            }

            result_table = conn->query(sql);

            // Create concatenated index if not ignoring
            IndexPtr final_index;
            if (!options.ignore_index) {
                std::vector<std::shared_ptr<arrow::Array>> index_arrays;
                for (const auto& idx : indices) {
                    index_arrays.push_back(idx->array().value());
                }
                auto concat_index = AssertResultIsOk(arrow::Concatenate(index_arrays));
                final_index = factory::index::make_index(concat_index, std::nullopt, "");
            } else {
                final_index = factory::index::from_range(result_table->num_rows());
            }

            frame = DataFrame(final_index, result_table);

        } else {
            // Column concatenation using JOIN
            std::string join_type = (options.joinType == JoinType::Inner) ? "INNER" : "FULL OUTER";

            if (table_names.size() == 2) {
                // Build explicit column list to avoid duplicating __index__
                // Use COALESCE to handle null values in __index__ from FULL OUTER JOIN
                sql = "SELECT COALESCE(" + table_names[0] + ".__index__, " +
                      table_names[1] + ".__index__) AS __index__";

                // Add columns from first table (excluding __index__)
                for (const auto& col : column_names_list[0]) {
                    sql += ", " + table_names[0] + ".\"" + col + "\"";
                }

                // Add columns from second table (excluding __index__)
                for (const auto& col : column_names_list[1]) {
                    sql += ", " + table_names[1] + ".\"" + col + "\"";
                }

                sql += " FROM " + table_names[0] + " " + join_type +
                      " JOIN " + table_names[1] + " ON " + table_names[0] +
                      ".__index__ = " + table_names[1] + ".__index__" +
                      " ORDER BY __index__";
            } else {
                // Multiple tables - chain joins, need to build column list carefully
                sql = "WITH ";

                // First CTE includes all columns from first table
                sql += "base AS (SELECT * FROM " + table_names[0] + ")";

                for (size_t i = 1; i < table_names.size(); i++) {
                    sql += ", step" + std::to_string(i) + " AS (SELECT ";

                    // Select all columns from previous step
                    if (i == 1) {
                        sql += "base.*";
                    } else {
                        sql += "step" + std::to_string(i-1) + ".*";
                    }

                    // Add new columns from current table (excluding __index__)
                    for (const auto& col : column_names_list[i]) {
                        sql += ", " + table_names[i] + ".\"" + col + "\"";
                    }

                    sql += " FROM ";
                    if (i == 1) {
                        sql += "base " + join_type + " JOIN " + table_names[i] +
                               " ON base.__index__ = " + table_names[i] + ".__index__";
                    } else {
                        sql += "step" + std::to_string(i-1) + " " + join_type +
                               " JOIN " + table_names[i] + " ON step" + std::to_string(i-1) +
                               ".__index__ = " + table_names[i] + ".__index__";
                    }
                    sql += ")";
                }
                sql += " SELECT * FROM step" + std::to_string(table_names.size()-1) +
                      " ORDER BY __index__";
            }

            result_table = conn->query(sql);

            // Extract index from result and remove __index__ column
            if (!options.ignore_index) {
                auto index_col_idx = result_table->schema()->GetFieldIndex("__index__");
                if (index_col_idx >= 0) {
                    auto index_array = result_table->column(index_col_idx);
                    auto index = factory::index::make_index(
                        factory::array::make_contiguous_array(index_array), std::nullopt, "");
                    result_table = AssertResultIsOk(result_table->RemoveColumn(index_col_idx));
                    frame = DataFrame(index, result_table);
                } else {
                    // Fallback to range index
                    frame = DataFrame(factory::index::from_range(result_table->num_rows()),
                                    result_table);
                }
            } else {
                // Remove __index__ column if it exists
                auto index_col_idx = result_table->schema()->GetFieldIndex("__index__");
                if (index_col_idx >= 0) {
                    result_table = AssertResultIsOk(result_table->RemoveColumn(index_col_idx));
                }
                frame = DataFrame(factory::index::from_range(result_table->num_rows()),
                                result_table);
            }
        }

        if (options.sort)
        {
            return options.axis == AxisType::Column ? frame.sort_index() : frame.sort_columns();
        }
        return frame;
    }

    DataFrame merge(const MergeOptions& options)
    {
        // Use DuckDB for merge operations
        auto conn = std::make_unique<CAPIConnection>();

        // Convert to DataFrames
        DataFrame df_left  = options.left.to_frame();
        DataFrame df_right = options.right.to_frame();

        // Register tables with DuckDB
        // Add index as column if needed
        auto left_table = df_left.table();
        auto right_table = df_right.table();

        if (!options.ignore_index) {
            auto left_index_array = df_left.index()->array().as_chunked_array();
            auto left_index_field = arrow::field("__left_index__", left_index_array->type());
            left_table = AssertResultIsOk(
                left_table->AddColumn(0, left_index_field, left_index_array));

            auto right_index_array = df_right.index()->array().as_chunked_array();
            auto right_index_field = arrow::field("__right_index__", right_index_array->type());
            right_table = AssertResultIsOk(
                right_table->AddColumn(0, right_index_field, right_index_array));
        }

        conn->registerArrowTable("left_table", left_table);
        conn->registerArrowTable("right_table", right_table);

        // Build SQL query based on axis and join type
        std::string sql;

        if (options.axis == AxisType::Row) {
            // Row-based merge (similar to concat for rows)
            sql = "SELECT * FROM left_table UNION ALL SELECT * FROM right_table";
        } else {
            // Column-based merge (join operation)
            std::string join_clause;
            switch (options.joinType) {
                case JoinType::Inner:
                    join_clause = "INNER";
                    break;
                case JoinType::Outer:
                    join_clause = "FULL OUTER";
                    break;
                default:
                    join_clause = "INNER";
            }

            // For column merge, use index-based join if indices are present
            if (!options.ignore_index) {
                sql = "SELECT * FROM left_table " + join_clause +
                      " JOIN right_table ON left_table.__left_index__ = right_table.__right_index__";
            } else {
                // Without index, do a cross join (cartesian product) for column concatenation
                sql = "SELECT * FROM left_table CROSS JOIN right_table";
            }
        }

        // Execute query
        auto result_table = conn->query(sql);

        // Handle index reconstruction
        IndexPtr final_index;
        if (!options.ignore_index) {
            if (options.axis == AxisType::Row) {
                // For row merge, concatenate indices
                std::vector<std::shared_ptr<arrow::Array>> index_arrays = {
                    df_left.index()->array().value(),
                    df_right.index()->array().value()
                };
                auto concat_index = AssertResultIsOk(arrow::Concatenate(index_arrays));
                final_index = factory::index::make_index(concat_index, std::nullopt, "");
            } else {
                // For column merge, use left index or merged index
                auto left_idx_col = result_table->schema()->GetFieldIndex("__left_index__");
                auto right_idx_col = result_table->schema()->GetFieldIndex("__right_index__");

                if (left_idx_col >= 0) {
                    auto index_array = result_table->column(left_idx_col);
                    final_index = factory::index::make_index(
                        factory::array::make_contiguous_array(index_array), std::nullopt, "");

                    // Remove index columns from result
                    if (left_idx_col >= 0) {
                        result_table = AssertResultIsOk(result_table->RemoveColumn(left_idx_col));
                    }
                    if (right_idx_col >= 0) {
                        // Adjust index after first removal
                        right_idx_col = result_table->schema()->GetFieldIndex("__right_index__");
                        if (right_idx_col >= 0) {
                            result_table = AssertResultIsOk(result_table->RemoveColumn(right_idx_col));
                        }
                    }
                } else {
                    final_index = df_left.index();
                }
            }
        } else {
            final_index = factory::index::from_range(result_table->num_rows());
        }

        DataFrame result(final_index, result_table);

        if (options.sort) {
            return options.axis == AxisType::Column ? result.sort_index() : result.sort_columns();
        }

        return result;
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
