//
// Created by Claude Code
//

#pragma once

#include "epoch_frame/aliases.h"
#include "epoch_frame/dataframe.h"
#include "epoch_frame/frame_or_series.h"
#include <vector>
#include <arrow/api.h>
#include <arrow/acero/api.h>

namespace epoch_frame {

/**
 * Concatenator - Stateful class for concatenating DataFrames/Series
 *
 * Holds the input frames and concatenation options, then executes the
 * concat operation with automatic optimization based on index alignment.
 */
class Concatenator {
public:
    /**
     * Constructor: Store frames and options for later execution
     */
    Concatenator(
        std::vector<FrameOrSeries> frames,
        JoinType join_type,
        AxisType axis,
        bool ignore_index,
        bool sort
    );

    /**
     * Execute the concatenation with automatic optimization
     * @return Concatenated DataFrame
     */
    DataFrame execute();

private:
    // Member variables (state)
    std::vector<FrameOrSeries> frames_;
    JoinType join_type_;
    AxisType axis_;
    bool ignore_index_;
    bool sort_;

    // Private implementation methods
    DataFrame concat_columns();
    DataFrame concat_rows();

    /**
     * Column concatenation with three optimization paths:
     * 1. Fast path: All indices identical (skip join)
     * 2. Pipelined path: Misaligned indices with streaming join+coalesce+sort
     * 3. Fallback path: For INNER joins or edge cases
     */
    std::tuple<arrow::TablePtr, arrow::ChunkedArrayPtr>
    concat_columns_impl(
        std::vector<arrow::TablePtr> const& tables,
        std::vector<IndexPtr> const& indices,
        std::string const& indexName
    );

    /**
     * Fast path: All indices are identical
     * Returns (merged_table, final_index_array)
     */
    std::tuple<arrow::TablePtr, arrow::ChunkedArrayPtr>
    concat_aligned_indices(
        std::vector<arrow::TablePtr> const& tables,
        std::vector<IndexPtr> const& indices
    );

    /**
     * Optimized path: Misaligned indices with pipelined join+coalesce+sort
     * Builds Acero declaration tree: join -> project(coalesce) -> sort
     * Returns (merged_table, final_index_array)
     */
    std::tuple<arrow::TablePtr, arrow::ChunkedArrayPtr>
    concat_misaligned_pipelined(
        std::vector<arrow::TablePtr> const& tables,
        std::vector<IndexPtr> const& indices,
        std::string const& indexName
    );

    /**
     * Fallback path: For INNER joins or when pipelined path not applicable
     * Uses materialized sequential joins
     * Returns (merged_table, final_index_array)
     */
    std::tuple<arrow::TablePtr, arrow::ChunkedArrayPtr>
    concat_fallback(
        std::vector<arrow::TablePtr> const& tables,
        std::vector<IndexPtr> const& indices,
        std::string const& indexName
    );

    // Helper methods

    /**
     * Build Acero join declaration tree for multiple tables
     */
    static arrow::acero::Declaration build_join_tree(
        std::vector<arrow::TablePtr> const& tables_with_index,
        arrow::acero::JoinType join_type,
        std::string const& index_name
    );

    /**
     * Coalesce multiple index columns (with suffixes) into one
     */
    static arrow::ChunkedArrayPtr coalesce_index_columns(
        arrow::TablePtr const& merged,
        std::string const& index_name
    );

    /**
     * Remove all index columns (with various suffixes) from table
     */
    static arrow::TablePtr remove_index_columns(
        arrow::TablePtr const& table,
        std::string const& index_name
    );

    /**
     * Generate unique index column name to avoid collisions
     */
    static std::string get_unique_index_column_name(
        std::vector<arrow::TablePtr> const& tables
    );
};

} // namespace epoch_frame
