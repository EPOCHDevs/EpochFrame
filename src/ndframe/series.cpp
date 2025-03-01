//
// Created by adesola on 2/16/25.
//
#include "epochframe/series.h"
#include "factory/index_factory.h"
#include "common/asserts.h"
#include "index/index.h"
#include <arrow/api.h>
#include <tabulate/table.hpp>
#include "epochframe/dataframe.h"
#include "methods/arith.h"
#include <arrow/chunked_array.h>
#include <factory/array_factory.h>
#include <vector_functions/arrow_vector_functions.h>

#include "index/index.h"
#include "common/table_or_array.h"
#include "common/methods_helper.h"

namespace epochframe {
    // ------------------------------------------------------------------------
    // Constructors / Destructor / Assignment
    // ------------------------------------------------------------------------
    Series::Series() = default;

    Series::Series(IndexPtr index, arrow::ChunkedArrayPtr array, const std::optional<std::string> &name):NDFrame<Series, arrow::ChunkedArray>(index, array), m_name(name) {}

    Series::Series(IndexPtr index, arrow::ArrayPtr array, const std::optional<std::string> &name) :
            Series(index, AssertArrayResultIsOk(arrow::ChunkedArray::Make({array})), name) {}

    Series::Series(arrow::ArrayPtr const &data, std::optional<std::string> const &name):Series(factory::index::from_range(0, data->length()), data, name) {}

    Series::Series(arrow::ChunkedArrayPtr const &data, std::optional<std::string> const &name):Series(factory::index::from_range(0, data->length()), data, name) {}

    // ------------------------------------------------------------------------
    // General Attributes
    // ------------------------------------------------------------------------

    DataFrame Series::to_frame(std::optional<std::string> const &name) const {
        return DataFrame(m_index,
                       arrow::Table::Make(
                               arrow::schema({arrow::field(name.value_or(m_name.value_or("")), m_table->type())}),
                               {m_table}));
    }

    //--------------------------------------------------------------------------
    // 2) Basic arithmetic: +, -, *, / with NDFrame and Scalar
    //--------------------------------------------------------------------------

    DataFrame Series::operator+(DataFrame const &other) const {
        return other.from_base(m_arithOp->add(other.tableComponent()));
    }

    DataFrame Series::operator-(DataFrame const &other) const {
        return other.from_base(m_arithOp->subtract(other.tableComponent()));
    }

    DataFrame Series::operator*(DataFrame const &other) const {
        return other.from_base(m_arithOp->multiply(other.tableComponent()));
    }

    DataFrame Series::operator/(DataFrame const &other) const {
        return other.from_base(m_arithOp->divide(other.tableComponent()));
    }

    //--------------------------------------------------------------------------
    // 3) Exponential, Power, sqrt, logs, trig
    //--------------------------------------------------------------------------

    DataFrame Series::power(DataFrame const &other) const {
        return other.from_base(m_arithOp->power(other.tableComponent()));
    }

    DataFrame Series::logb(DataFrame const &other) const {
        return other.from_base(m_arithOp->logb(other.tableComponent()));
    }

    //--------------------------------------------------------------------------
    // 4) Bitwise ops
    //--------------------------------------------------------------------------

    DataFrame Series::bitwise_and(DataFrame const &other) const {
        return other.from_base(m_arithOp->bit_wise_and(other.tableComponent()));
    }

    DataFrame Series::bitwise_or(DataFrame const &other) const {
        return other.from_base(m_arithOp->bit_wise_or(other.tableComponent()));
    }

    DataFrame Series::bitwise_xor(DataFrame const &other) const {
        return other.from_base(m_arithOp->bit_wise_xor(other.tableComponent()));
    }

    DataFrame Series::shift_left(DataFrame const &other) const {
        return other.from_base(m_arithOp->shift_left(other.tableComponent()));
    }

    DataFrame Series::shift_right(DataFrame const &other) const {
        return other.from_base(m_arithOp->shift_right(other.tableComponent()));
    }

    //--------------------------------------------------------------------------
    // 8) Indexing ops
    //--------------------------------------------------------------------------

    Scalar Series::iloc(int64_t row) const {
        row = resolve_integer_index(row, m_table->length());
        return Scalar(AssertResultIsOk(m_table->GetScalar(row)));
    }

    Scalar Series::loc(const Scalar &index_label) const {
        return iloc(m_index->get_loc(index_label));
    }

    Series Series::loc(const SeriesToSeriesCallable &callable) const {
        return NDFrame<Series, arrow::ChunkedArray>::loc(callable(*this));
    }

    //--------------------------------------------------------------------------
    // 13) Selection & Transform
    //--------------------------------------------------------------------------
    arrow::ArrayPtr Series::unique() const {
        return vector::unique(factory::array::make_contiguous_array(arrow::Datum{m_table}));
    }

    //--------------------------------------------------------------------------
    // Serialization
    //--------------------------------------------------------------------------

    std::ostream &operator<<(std::ostream &os, Series const &df) {
        tabulate::Table table;
        tabulate::Table::Row_t header{fmt::format("index({})", df.m_index->dtype()->ToString())};
        header.push_back(fmt::format("{}({})", df.m_name.value_or(""), df.m_table->type()->ToString()));
        table.add_row(header);
        for (int64_t i = 0; i < df.m_index->size(); ++i) {
            auto index = df.m_index->array()->GetScalar(i).MoveValueUnsafe()->ToString();
            tabulate::Table::Row_t row{index};
            row.push_back(df.m_table->GetScalar(i).MoveValueUnsafe()->ToString());
            table.add_row(row);
        }
        os << table;
        return os;
    }

    Series Series::from_base(IndexPtr const &index, ArrowPtrType const &table) const  {
        return Series(index, table, m_name);
    }

    Series Series::from_base(TableComponent const &tableComponent) const {
        return Series(tableComponent.first, tableComponent.second.chunked_array(), m_name);
    }
}
