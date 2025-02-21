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
#include "index/arrow_index.h"
#include "common/table_or_array.h"

namespace epochframe {
    MethodBase::MethodBase(const TableComponent& data): m_data(data) {
        if (m_data.second.is_table()) {
           AssertWithTraceFromStream(m_data.second.table(), "Table is nullptr");
        }
        else if (m_data.second.is_chunked_array()) {
            AssertWithTraceFromStream(m_data.second.chunked_array(), "ChunkedArray is nullptr");
        }
    }
    //----------------------------------------------------------------------
// apply(op, FunctionOptions): unary function with custom options
// (e.g. "round" with RoundOptions).
//----------------------------------------------------------------------
    TableOrArray
    MethodBase::apply(std::string const &op,
                      const arrow::compute::FunctionOptions *options) const {
        auto [index, data] = m_data;

        if (data.is_chunked_array()) {
            return TableOrArray{arrow_utils::call_unary_compute_array(data.datum(), op, options)};
        }

        return TableOrArray{arrow_utils::apply_function_to_table(data.table(), [&](const arrow::Datum &column, std::string const& _) {
            return arrow_utils::call_unary_compute_array(column, op, options);
        })};
    }

//----------------------------------------------------------------------
// apply(op, Scalar|Series): binary function with a single scalar
// (e.g. df + 10, df * 2).
//----------------------------------------------------------------------
    TableOrArray
    MethodBase::apply(std::string const &op, const arrow::Datum &other, bool lhs) const {
        AssertWithTraceFromStream(other.kind() == arrow::Datum::SCALAR || other.kind() == arrow::Datum::CHUNKED_ARRAY || other.kind() == arrow::Datum::ARRAY, "Invalid other type");

        auto [index, data] = m_data;
        auto get_lhs_or_rhs = [&] (arrow::Datum const& column) {
            return lhs ? std::vector{column, other} : std::vector{other, column};
        };

        if (data.is_chunked_array()) {
            return TableOrArray{arrow_utils::call_compute_array(get_lhs_or_rhs(data.datum()), op)};
        }

        auto table = data.table();

        auto schema = table->schema();

        std::vector<arrow::ChunkedArrayPtr> new_arrays;
        new_arrays.resize(schema->num_fields());

        return TableOrArray{arrow_utils::apply_function_to_table(table, [&](const arrow::Datum &column, std::string const&) {
            return arrow_utils::call_compute_array(get_lhs_or_rhs(column), op);
        })};
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

        if (left_index->equals(right_index))
        {
            if (left_rb.is_chunked_array() && right_rb.is_chunked_array()) {
                return TableComponent{left_index, arrow_utils::call_compute_array(std::vector{left_rb.datum(), right_rb.datum()}, op)};
            }

            if (left_rb.is_table() && right_rb.is_table() && left_rb.table()->schema()->Equals(right_rb.table()->schema())) {
                return TableComponent{left_index, unsafe_binary_op(left_rb.table(), right_rb.table(), op)};
            }
        }

        // We'll call a helper that handles the Arrow "hash_join" behind the scenes:
        auto [new_index, aligned_left, aligned_right] = align_by_index_and_columns(m_data, otherData);

        // Now we have two RecordBatches with the same row count and the same ordering of 'index'.
        // Perform the actual binary operation column by column.
        auto out = unsafe_binary_op(aligned_left, aligned_right, op);
        return TableComponent{new_index, out};
    }

    arrow::TablePtr MethodBase::merge_index() const {
        return add_column(m_data.second.get_table("__series_MethodBase__"), RESERVED_INDEX_NAME, m_data.first->array());
    }

    TableComponent MethodBase::unzip_index(arrow::TablePtr const &table) const {
        auto field_index = table->schema()->GetFieldIndex(RESERVED_INDEX_NAME);
        AssertFalseFromStream(field_index == -1, "table schema can not reference index name");

        auto newIndex = table->column(field_index);
        auto newTable = table->RemoveColumn(field_index);
        AssertWithTraceFromStream(newTable.ok(), "unzip index failed");

        return TableComponent{std::make_shared<ArrowIndex>(newIndex), TableOrArray{newTable.MoveValueUnsafe()}};
    }

    TableOrArray MethodBase::rapply(std::string const &op, const arrow::Datum &other) const {
            return apply(op, other, false);
    }
}
