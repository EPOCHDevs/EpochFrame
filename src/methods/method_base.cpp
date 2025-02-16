//
// Created by adesola on 2/15/25.
//

#include "method_base.h"
#include <arrow/api.h>
#include <arrow/compute/api.h>
#include <epoch_lab_shared/macros.h>    // your assert/throw utilities
#include <unordered_map>
#include <stdexcept>
#include <range/v3/view/zip.hpp>
#include "common/methods_helper.h"
#include "epochframe/scalar.h"
#include "index/index.h"
#include "common/arrow_compute_utils.h"
#include <tbb/parallel_for_each.h>
#include <range/v3/view/enumerate.hpp>
#include <iostream>

namespace epochframe {
    //----------------------------------------------------------------------
// apply(op, FunctionOptions): unary function with custom options
// (e.g. "round" with RoundOptions).
//----------------------------------------------------------------------
    arrow::TablePtr
    MethodBase::apply(std::string const &op,
                      const arrow::compute::FunctionOptions *options) const {
        auto [index, rb] = m_data;
        if (!rb) {
            throw std::runtime_error("Arithmetic::apply(op, options): null RecordBatch");
        }

        auto schema = rb->schema();

        std::vector<arrow::ChunkedArrayPtr> new_arrays;
        new_arrays.resize(schema->num_fields());

        tbb::parallel_for(0, schema->num_fields(), [&](int64_t i) {
            new_arrays[i] = arrow_utils::call_unary_compute_array(rb->column(i), op, options);
        });

        auto new_rb = arrow::Table::Make(rb->schema(), new_arrays);
        return new_rb;
    }

//----------------------------------------------------------------------
// apply(op, Scalar): binary function with a single scalar
// (e.g. df + 10, df * 2).
//----------------------------------------------------------------------
    arrow::TablePtr
    MethodBase::apply(std::string const &op, const Scalar &other, bool lhs) const {
        auto [index, rb] = m_data;
        if (!rb) {
            throw std::runtime_error("Arithmetic::apply(scalar): null RecordBatch");
        }

        // Convert epochframe::Scalar -> arrow::ScalarPtr
        arrow::ScalarPtr arrow_scalar = other.value();
        if (!arrow_scalar) {
            throw std::runtime_error("Arithmetic::apply(scalar): null arrow scalar");
        }

        auto schema = rb->schema();

        std::vector<arrow::ChunkedArrayPtr> new_arrays;
        new_arrays.resize(schema->num_fields());

        tbb::parallel_for(0, schema->num_fields(), [&](int64_t i) {
            auto const &column = rb->column(i);
            new_arrays[i] = arrow_utils::call_compute_array(lhs ? std::vector<arrow::Datum>{column, arrow_scalar}
                                                                : std::vector<arrow::Datum>{arrow_scalar, column}, op);
        });

        auto new_rb = arrow::Table::Make(rb->schema(), new_arrays);
        return new_rb;
    }

//----------------------------------------------------------------------
// apply(op, TableComponent): binary function on two RecordBatches
// (df + df). The main difference from Pandas is we must do *index alignment*.
//
// We'll do a FULL_OUTER join on the index, then do column-wise arithmetic
// for columns with the same name. This is the simplest "Pandas-like" approach.
//----------------------------------------------------------------------
    TableComponent
    MethodBase::apply(std::string const &op, const TableComponent &otherData) const {
        auto left_index = m_data.first;
        auto right_index = otherData.first;

        if (!left_index || !right_index) {
            throw std::runtime_error("Arithmetic::apply(TableComponent): index pointer is null");
        }

        // 1) If indexes already match in shape & order, skip alignment
        //    (In a real system, you'd do a proper check: are they the same length,
        //     and do they have identical labels? If so, just do direct columnwise op.)
        //
        // 2) Otherwise, do a full outer join on the "index" column.
        auto left_rb = m_data.second;
        auto right_rb = otherData.second;
        if (!left_rb || !right_rb) {
            throw std::runtime_error("Null RecordBatch in apply(TableComponent)");
        }

        if (left_index->equals(right_index) && left_rb->schema()->Equals(right_rb->schema())) {
            return {left_index, unsafe_binary_op(left_rb, right_rb, op)};
        }

        // We'll call a helper that handles the Arrow "hash_join" behind the scenes:
        auto [new_index, aligned_left, aligned_right] = align_by_index_and_columns(m_data, otherData);

        // Now we have two RecordBatches with the same row count and the same ordering of 'index'.
        // Perform the actual binary operation column by column.
        auto out = unsafe_binary_op(aligned_left, aligned_right, op);
        return {new_index, out};
    }
}
