//
// Created by Claude Code
//

#include "concatenator.h"
#include "methods_helper.h"
#include "common/exceptions.h"
#include "arrow_compute_utils.h"
#include "epoch_frame/factory/index_factory.h"
#include "epoch_frame/factory/array_factory.h"
#include "epoch_frame/factory/dataframe_factory.h"
#include <spdlog/spdlog.h>
#include <arrow/acero/api.h>
#include <arrow/compute/api.h>

namespace epoch_frame {

namespace ac = arrow::acero;
namespace cp = arrow::compute;

// Constructor
Concatenator::Concatenator(
    std::vector<FrameOrSeries> frames,
    JoinType join_type,
    AxisType axis,
    bool ignore_index,
    bool sort)
    : frames_(std::move(frames))
    , join_type_(join_type)
    , axis_(axis)
    , ignore_index_(ignore_index)
    , sort_(sort)
{
}

// Main execution method
DataFrame Concatenator::execute() {
    // Validate input
    if (frames_.empty()) {
        throw std::runtime_error("concat: no frames to concatenate");
    }

    // Early exit for single frame
    if (frames_.size() == 1) {
        return frames_.front().to_frame();
    }

    // Remove empty objects
    std::vector<FrameOrSeries> cleanedObjs = remove_empty_objs(frames_);
    if (cleanedObjs.empty()) {
        return make_empty_dataframe(frames_.front().index()->dtype());
    }
    if (cleanedObjs.size() == 1) {
        return (join_type_ == JoinType::Inner)
                   ? make_empty_dataframe(frames_.front().index()->dtype())
                   : cleanedObjs.front().to_frame();
    }

    // Update frames to cleaned version
    frames_ = cleanedObjs;

    // Dispatch to appropriate concat function
    DataFrame frame = (axis_ == AxisType::Row)
        ? concat_rows()
        : concat_columns();

    // Apply sorting if requested (mirror original concat semantics)
    if (sort_) {
        return (axis_ == AxisType::Column)
            ? frame.sort_index()
            : frame.sort_columns();
    }
    return frame;
}

// Column concatenation (main logic)
DataFrame Concatenator::concat_columns() {
    // Prepare inputs - convert FrameOrSeries to working data structures
    auto [dataframes, indices, tables] = prepare_concat_inputs(frames_);

    // Generate unique index column name to avoid collisions with existing columns
    const std::string indexName = get_unique_index_column_name(tables);

    // Check for duplicate column names
    auto duplicate_columns = check_duplicate_columns(tables);
    bool has_duplicates = !duplicate_columns.empty();

    // For inner join with duplicates, check if indices overlap
    if (has_duplicates && join_type_ == JoinType::Inner) {
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

    // Execute the concat with automatic optimization
    auto [merged, final_index_array] = concat_columns_impl(tables, indices, indexName);

    // Create final DataFrame with reconstructed index
    if (!ignore_index_ && final_index_array) {
        auto final_index = factory::index::make_index(
            factory::array::make_contiguous_array(final_index_array),
            std::nullopt,
            "");
        return DataFrame(final_index, merged);
    }

    return DataFrame(factory::index::from_range(merged->num_rows()), merged);
}

// Row concatenation (delegates to existing implementation)
DataFrame Concatenator::concat_rows() {
    // Prepare inputs
    auto [dataframes, indices, tables] = prepare_concat_inputs(frames_);

    // Delegate to existing concat_rows_acero function in methods_helper.cpp
    // TODO: Could move this logic into Concatenator as well if desired
    return concat_rows_acero(tables, indices, ignore_index_);
}

// Column concatenation implementation with three optimization paths
std::tuple<arrow::TablePtr, arrow::ChunkedArrayPtr>
Concatenator::concat_columns_impl(
    std::vector<arrow::TablePtr> const& tables,
    std::vector<IndexPtr> const& indices,
    std::string const& indexName)
{
    // Fast path: Check if all indices are identical
    bool all_indices_identical = true;
    if (join_type_ == JoinType::Outer && indices.size() > 1) {
        for (size_t i = 1; i < indices.size(); i++) {
            if (!indices[i]->equals(indices[0])) {
                all_indices_identical = false;
                break;
            }
        }
    } else {
        all_indices_identical = false;
    }

    // Choose optimization path based on index alignment
    if (all_indices_identical) {
        return concat_aligned_indices(tables, indices);
    } else if (join_type_ == JoinType::Outer && indices.size() > 1) {
        return concat_misaligned_pipelined(tables, indices, indexName);
    } else {
        return concat_fallback(tables, indices, indexName);
    }
}

// Fast path: All indices are identical
std::tuple<arrow::TablePtr, arrow::ChunkedArrayPtr>
Concatenator::concat_aligned_indices(
    std::vector<arrow::TablePtr> const& tables,
    std::vector<IndexPtr> const& indices)
{
    SPDLOG_DEBUG("Concatenator: Fast path - all {} indices identical, skipping Acero join",
                 tables.size());

    std::vector<arrow::ChunkedArrayPtr> all_columns;
    std::vector<std::shared_ptr<arrow::Field>> all_fields;

    for (const auto& table : tables) {
        for (int i = 0; i < table->num_columns(); i++) {
            all_columns.push_back(table->column(i));
            all_fields.push_back(table->schema()->field(i));
        }
    }

    arrow::TablePtr merged = arrow::Table::Make(arrow::schema(all_fields), all_columns);
    arrow::ChunkedArrayPtr final_index_array = indices[0]->array().as_chunked_array();

    return {merged, final_index_array};
}

// Fixed: Misaligned indices - precompute union index then align each table
std::tuple<arrow::TablePtr, arrow::ChunkedArrayPtr>
Concatenator::concat_misaligned_pipelined(
    std::vector<arrow::TablePtr> const& tables,
    std::vector<IndexPtr> const& indices,
    std::string const& /* indexName */)
{
    SPDLOG_DEBUG("CONCAT_DEBUG: Using FIXED approach - precomputing union index for {} tables", tables.size());

    // LOG: Input table info
    for (size_t i = 0; i < tables.size(); i++) {
        SPDLOG_DEBUG("CONCAT_DEBUG: Input Table {}: {} rows, {} cols, index length: {}",
                    i, tables[i]->num_rows(), tables[i]->num_columns(), indices[i]->size());
    }

    // Validate: Check for duplicate indices in input dataframes (matching pandas behavior)
    // Pandas raises: InvalidIndexError: Reindexing only valid with uniquely valued Index objects
    for (size_t i = 0; i < indices.size(); i++) {
        if (indices[i]->has_duplicates()) {
            throw std::invalid_argument(
                "Cannot perform column-wise concat with duplicate index values. "
                "Input dataframe at position " + std::to_string(i) + " has duplicate indices. "
                "Reindexing is only valid with uniquely valued Index objects.");
        }
    }

    // Step 1: Compute the union of all indices (deduplicated)
    IndexPtr merged_index = indices[0];
    for (size_t i = 1; i < indices.size(); i++) {
        SPDLOG_DEBUG("CONCAT_DEBUG: Before union: merged_index size={}, indices[{}] size={}",
                   merged_index->size(), i, indices[i]->size());
        merged_index = merged_index->union_(indices[i]);
        SPDLOG_DEBUG("CONCAT_DEBUG: After union with table {}: merged_index size={}",
                   i, merged_index->size());
    }

    SPDLOG_DEBUG("CONCAT_DEBUG: Merged index computed: {} unique values", merged_index->size());

    // Step 2: Build a canonical aligned index order (sorted by value)
    auto union_array = merged_index->array().as_chunked_array();
    auto index_table = arrow::Table::Make(
        arrow::schema({arrow::field("__idx", union_array->type())}),
        {union_array});

    auto sort_indices = AssertResultIsOk(
        SortIndices(index_table,
                    arrow::SortOptions{{arrow::compute::SortKey{"__idx"}}}));

    auto sorted_union_array = AssertArrayResultIsOk(
        arrow::compute::Take(union_array, sort_indices));

    auto aligned_index = factory::index::make_index(
        factory::array::make_contiguous_array(sorted_union_array),
        std::nullopt,
        merged_index->name());

    // Step 3: Align each table to the aligned index
    std::vector<arrow::TablePtr> aligned_tables;
    for (size_t i = 0; i < tables.size(); i++) {
        TableComponent table_component{indices[i], TableOrArray{tables[i]}};
        auto aligned = align_by_index(table_component, aligned_index, Scalar{});
        aligned_tables.push_back(aligned.get_table(""));

        SPDLOG_DEBUG("CONCAT_DEBUG: Aligned Table {}: {} rows (was {})",
                    i, aligned_tables[i]->num_rows(), tables[i]->num_rows());
    }

    // Step 4: Concatenate aligned tables column-wise (simple horizontal concat)
    std::vector<arrow::ChunkedArrayPtr> all_columns;
    std::vector<std::shared_ptr<arrow::Field>> all_fields;

    for (const auto& table : aligned_tables) {
        for (int i = 0; i < table->num_columns(); i++) {
            all_columns.push_back(table->column(i));
            all_fields.push_back(table->schema()->field(i));
        }
    }

    arrow::TablePtr merged = arrow::Table::Make(arrow::schema(all_fields), all_columns);
    auto final_index_array = aligned_index->array().as_chunked_array();

    SPDLOG_DEBUG("CONCAT_DEBUG: Final result: {} rows, {} cols", merged->num_rows(), merged->num_columns());

    return {merged, final_index_array};
}

// Fallback path: For INNER joins or when pipelined path not applicable
std::tuple<arrow::TablePtr, arrow::ChunkedArrayPtr>
Concatenator::concat_fallback(
    std::vector<arrow::TablePtr> const& tables,
    std::vector<IndexPtr> const& indices,
    std::string const& indexName)
{
    SPDLOG_DEBUG("Concatenator: Fallback path - using Acero join for {} tables", tables.size());

    // Add index as column to all tables
    std::vector<arrow::TablePtr> tables_with_index(tables.size());
    for (size_t i = 0; i < tables.size(); i++) {
        auto index_array = indices[i]->array().as_chunked_array();
        auto index_field = arrow::field(indexName, index_array->type());
        tables_with_index[i] = AssertResultIsOk(
            tables[i]->AddColumn(0, index_field, index_array));
    }

    // Build and execute join plan
    ac::JoinType acero_join_type = (join_type_ == JoinType::Inner)
                            ? ac::JoinType::INNER
                            : ac::JoinType::FULL_OUTER;

#ifndef NDEBUG
    size_t total_input_rows = 0;
    for (const auto& table : tables_with_index) {
        total_input_rows += table->num_rows();
    }
    SPDLOG_DEBUG("Concatenator: Starting multi-way join - {} tables, {} total input rows",
                 tables_with_index.size(), total_input_rows);
#endif

    auto join_plan = build_join_tree(tables_with_index, acero_join_type, indexName);
    arrow::TablePtr merged = AssertResultIsOk(ac::DeclarationToTable(join_plan));

    SPDLOG_DEBUG("Concatenator: Join completed - output {} rows × {} columns ({}MB estimated)",
                 merged->num_rows(), merged->num_columns(),
                 (merged->num_rows() * merged->num_columns() * 8) / (1024 * 1024));

    // Coalesce and sort by index
    arrow::ChunkedArrayPtr final_index_array = coalesce_index_columns(merged, indexName);
    if (final_index_array) {
        auto sort_indices = arrow_utils::call_compute_array({final_index_array}, "sort_indices");
        merged = AssertTableResultIsOk(arrow::compute::Take(merged, sort_indices));
        final_index_array = AssertArrayResultIsOk(
            arrow::compute::Take(final_index_array, sort_indices));
    }

    // Remove index columns
    merged = remove_index_columns(merged, indexName);

    return {merged, final_index_array};
}

// Static helper: Build Acero join declaration tree
arrow::acero::Declaration Concatenator::build_join_tree(
    std::vector<arrow::TablePtr> const& tables_with_index,
    arrow::acero::JoinType join_type,
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

    // For 3+ tables, build left-deep join tree
    ac::Declaration current = ac::Declaration{
        "table_source",
        ac::TableSourceNodeOptions(tables_with_index[0])
    };

    std::string current_index_col = index_name;

    for (size_t i = 1; i < tables_with_index.size(); i++) {
        ac::Declaration right_source{
            "table_source",
            ac::TableSourceNodeOptions(tables_with_index[i])
        };

        std::string left_suffix = "_T" + std::to_string(i);
        std::string right_suffix = "_T" + std::to_string(i + 1);

        ac::HashJoinNodeOptions join_opts{
            join_type,
            /*left_keys=*/{current_index_col},
            /*right_keys=*/{index_name},
            cp::literal(true),
            left_suffix,
            right_suffix
        };

        current = ac::Declaration{
            "hashjoin",
            {std::move(current), std::move(right_source)},
            std::move(join_opts)
        };

        current_index_col = current_index_col + left_suffix;
    }

    return current;
}

// Static helper: Coalesce multiple index columns
arrow::ChunkedArrayPtr Concatenator::coalesce_index_columns(
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

// Static helper: Remove index columns from table
arrow::TablePtr Concatenator::remove_index_columns(
    arrow::TablePtr const& merged,
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

    arrow::TablePtr result = merged;
    for (const auto& col_name : columns_to_remove) {
        auto col_idx = result->schema()->GetFieldIndex(col_name);
        if (col_idx >= 0) {
            result = AssertResultIsOk(result->RemoveColumn(col_idx));
        }
    }

    return result;
}

// Static helper: Generate unique index column name
std::string Concatenator::get_unique_index_column_name(
    std::vector<arrow::TablePtr> const& tables)
{
    // Delegate to existing implementation in methods_helper.cpp
    return epoch_frame::get_unique_index_column_name(tables);
}

} // namespace epoch_frame
