//
// NDFrame.cpp
// Created by adesola on 1/20/25.
//

#include "epochframe/dataframe.h"

#include <unordered_set>

#include "index/index.h"
#include "methods/arith.h"
#include "common/methods_helper.h"
#include "epochframe/series.h"
#include "epochframe/common.h"
#include "factory/index_factory.h"
#include "factory/array_factory.h"
#include <tabulate/table.hpp>
#include "methods/groupby.h"
#include "common/arrow_compute_utils.h"


namespace epochframe {
    // ------------------------------------------------------------------------
    // Constructors / Destructor / Assignment
    // ------------------------------------------------------------------------

    DataFrame::DataFrame(arrow::TablePtr const &data) : NDFrame(data) {
        auto columnNames = m_table->schema()->field_names();
        AssertFromStream(std::unordered_set(columnNames.begin(), columnNames.end()).size() == m_table->num_columns(), "duplicate columns are not permitted for dataframe: " << m_table->schema()->ToString());
    }

    DataFrame::DataFrame(IndexPtr const &index, arrow::TablePtr const &data) : NDFrame<DataFrame, arrow::Table>(index, data) {
        auto columnNames = m_table->schema()->field_names();
        AssertFromStream(std::unordered_set(columnNames.begin(), columnNames.end()).size() == m_table->num_columns(), "duplicate columns are not permitted for dataframe: " << m_table->schema()->ToString());
    }

    // ------------------------------------------------------------------------
    // General Attributes
    // ------------------------------------------------------------------------

    DataFrame DataFrame::add_prefix_or_suffix(const std::string &prefix_or_suffix, bool is_prefix) const {
        auto schema = m_table->schema();
        std::vector<arrow::FieldPtr> fields;
        for (const auto &field : schema->fields()) {
            auto new_name = is_prefix ? prefix_or_suffix + field->name() : field->name() + prefix_or_suffix;
            fields.push_back(arrow::field(new_name, field->type()));
        }
        auto new_schema = std::make_shared<arrow::Schema>(fields);
        return DataFrame{m_index, arrow::Table::Make(new_schema, m_table->columns())};
    }

    DataFrame DataFrame::rename(std::unordered_map<std::string, std::string> const& by) {
        std::vector<std::string> new_fields;
        for (auto const& field: m_table->schema()->fields()) {
            auto field_name = field->name();
            if (auto it = by.find(field_name); it != by.end()) {
                new_fields.push_back(it->second);
            }
            else {
                new_fields.push_back(field_name);
            }
        }
        return {
            m_index,
            AssertResultIsOk(m_table->RenameColumns(new_fields))
        };
    }

DataFrame DataFrame::set_index(std::string const & new_index) const {
        auto indexPos = m_table->schema()->GetFieldIndex(new_index);
        AssertFromStream(indexPos != -1, new_index << " is not a valid column");
        auto index = m_table->column(indexPos);
        auto new_table = AssertResultIsOk(m_table->RemoveColumn(indexPos));
        return DataFrame{m_index->Make(factory::array::make_contiguous_array(index)), new_table};
    }

    size_t DataFrame::num_rows() const {
        return m_table->num_rows();
    }

    size_t DataFrame::num_cols() const {
        return m_table->num_columns();
    }

    std::vector<std::string> DataFrame::column_names() const {
        return m_table->ColumnNames();
    }

    Series DataFrame::to_series() const {
        AssertFromStream(m_table->num_columns() == 1, "to_Series must be called on a single column table.");
        const auto column = m_table->column(0);
        return Series(m_index, column, m_table->field(0)->name());
    }

    //--------------------------------------------------------------------------
    // 2) Basic arithmetic: +, -, *, / with NDFrame and Scalar
    //--------------------------------------------------------------------------

    DataFrame DataFrame::operator+(Series const &other) const {
        return from_base(m_arithOp->add(other.tableComponent()));
    }

    DataFrame DataFrame::operator-(Series const &other) const {
        return from_base(m_arithOp->subtract(other.tableComponent()));
    }

    DataFrame DataFrame::operator*(Series const &other) const {
        return from_base(m_arithOp->multiply(other.tableComponent()));
    }

    DataFrame DataFrame::operator/(Series const &other) const {
        return from_base(m_arithOp->divide(other.tableComponent()));
    }

    //--------------------------------------------------------------------------
    // 3) Exponential, Power, sqrt, logs, trig
    //--------------------------------------------------------------------------

    DataFrame DataFrame::power(Series const &other) const {
        return from_base(m_arithOp->power(other.tableComponent()));
    }

    DataFrame DataFrame::logb(Series const &other) const {
        return from_base(m_arithOp->logb(other.tableComponent()));
    }

    //--------------------------------------------------------------------------
    // 4) Bitwise ops
    //--------------------------------------------------------------------------

    DataFrame DataFrame::bitwise_and(Series const &other) const {
        return from_base(m_arithOp->bit_wise_and(other.tableComponent()));
    }

    DataFrame DataFrame::bitwise_or(Series const &other) const {
        return from_base(m_arithOp->bit_wise_or(other.tableComponent()));
    }

    DataFrame DataFrame::bitwise_xor(Series const &other) const {
        return from_base(m_arithOp->bit_wise_xor(other.tableComponent()));
    }

    DataFrame DataFrame::shift_left(Series const &other) const {
        return from_base(m_arithOp->shift_left(other.tableComponent()));
    }

    DataFrame DataFrame::shift_right(Series const &other) const {
        return from_base(m_arithOp->shift_right(other.tableComponent()));
    }

    //////////////////////////////////////////////////////////////////////////
    /// 8) INDEXING OPS
    //////////////////////////////////////////////////////////////////////////
    Series DataFrame::iloc(int64_t row) const {
        row = resolve_integer_index(row, m_table->num_rows());
        arrow::ScalarVector cols(m_table->num_columns());
        std::ranges::transform(m_table->columns(), cols.begin(),
                               [&](const arrow::ChunkedArrayPtr &array) {
                                   return AssertResultIsOk(array->GetScalar(row));
                               });
        return Series(factory::index::make_object_index(m_table->ColumnNames()),
                      factory::array::make_array(std::move(cols), arrow::utf8()));
    }

    Scalar DataFrame::iloc(int64_t row, std::string const &col) const {
        row = resolve_integer_index(row, m_table->num_rows());
        auto column = get_column_by_name(*m_table, col);
        return Scalar(AssertResultIsOk(column->GetScalar(row)));
    }

    Series DataFrame::operator[](const std::string &column) const {
        const auto col = get_column_by_name(*m_table, column);
        return Series(m_index, col, column);
    }

    DataFrame DataFrame::operator[](const StringVector &columns) const {
        if (columns.empty()) {
            return DataFrame{};
        }

        std::vector<arrow::ChunkedArrayPtr> cols;
        arrow::FieldVector fields;
        for (const auto &col : columns) {
            fields.push_back(get_field_by_name(*m_table->schema(), col));
            cols.push_back(get_column_by_name(*m_table, col));
        }
        return DataFrame(m_index, arrow::Table::Make(arrow::schema(fields), cols));
    }

    DataFrame DataFrame::operator[](const StringVectorCallable & callable) const {
        return operator[](callable(m_table->ColumnNames()));
    }

    Series DataFrame::loc(const Scalar &index_label) const {
        return iloc(m_index->get_loc(index_label));
    }

    Scalar DataFrame::loc(const Scalar &index_label, const std::string &column) const {
        auto integer_index = m_index->get_loc(index_label);
        return Scalar(AssertResultIsOk(get_column_by_name(*m_table, column)->GetScalar(integer_index)));
    }

    DataFrame DataFrame::loc(const DataFrameToSeriesCallable& callable) const {
        return loc(callable(*this));
    }

    Series DataFrame::loc(const Scalar & scalar, const LocColArgumentVariant & locColArgumentVariant) const {
        auto rows = get_variant_column(*this, locColArgumentVariant);
        return rows.loc(scalar);
    }

    DataFrame DataFrame::loc(const LocRowArgumentVariant & locRowArgumentVariant, const LocColArgumentVariant & locColArgumentVariant) const {
        auto cols = get_variant_column(*this, locColArgumentVariant);
        return get_variant_row(cols, locRowArgumentVariant);
    }

    Series DataFrame::loc(const LocRowArgumentVariant & locRowArgumentVariant, const std::string & column) const {
        auto column_series = operator[](column);
        return get_variant_row(column_series, locRowArgumentVariant);
    }

    DataFrame DataFrame::operator[](const arrow::ArrayPtr& array) const {
        auto string_array = std::dynamic_pointer_cast<arrow::StringArray>(array);
        AssertFromStream(string_array, "NDFrame::operator[]: Array is not a string array");
        StringVector column_names;
        column_names.reserve(string_array->length());
        for (auto const& name : *string_array) {
            AssertFromStream(name, "NDFrame::operator[]: Array contains null values");
            column_names.emplace_back(*name);
        }
        return operator[](column_names);
    }

    DataFrame DataFrame::sort_columns(bool ascending) const {
        auto columns = m_table->ColumnNames();
        if (ascending) {
            std::ranges::sort(columns, std::less<std::string>());
        } else {
            std::ranges::sort(columns, std::greater<std::string>());
        }
        arrow::ChunkedArrayVector array_list(columns.size());
        arrow::FieldVector fields(columns.size());
        std::transform(columns.begin(), columns.end(), fields.begin(), array_list.begin(),
                       [&](std::string const &column, arrow::FieldPtr &field) {
                           field = m_table->schema()->GetFieldByName(column);
                           return m_table->GetColumnByName(column);
                       });
        return from_base(m_index, arrow::Table::Make(arrow::schema(fields), array_list));
    }
    //////////////////////////////////////////////////////////////////////////
    /// 10) SERIALIZATION
    //////////////////////////////////////////////////////////////////////////

    std::ostream &operator<<(std::ostream &os, DataFrame const &df) {
        try {
            tabulate::Table table;
            tabulate::Table::Row_t header{std::format("index({})", df.m_index->dtype()->ToString())};
            for (auto const &col : df.m_table->ColumnNames()) {
                header.emplace_back(std::format("{}({})", col, get_column_by_name(*df.m_table, col)->type()->ToString()));
            }

            table.add_row(header);
            for (int64_t i = 0; i < df.m_index->size(); ++i) {
                auto index = df.m_index->array().value()->GetScalar(i).MoveValueUnsafe()->ToString();
                tabulate::Table::Row_t row{index};
                for (auto const &col : df.m_table->ColumnNames()) {
                    row.push_back(get_column_by_name(*df.m_table, col)
                                          ->GetScalar(i)
                                          .MoveValueUnsafe()
                                          ->ToString());
                }
                table.add_row(row);
            }
            os << table;
        }catch (const std::exception &e) {
            os << "Failed to print table: " << e.what() << std::endl;
            os << "index\n" << df.m_index->array().value()->ToString() << std::endl;
            os << "table:\n" << df.m_table->ToString() << std::endl;
        }
        return os;
    }

    DataFrame DataFrame::from_base(IndexPtr const &index, arrow::TablePtr const &table) const  {
        return DataFrame(index, table);
    }

    DataFrame DataFrame::from_base(TableComponent const &tableComponent) const  {
        return DataFrame(tableComponent.first, tableComponent.second.table());
    }

    GroupByAgg<DataFrame> DataFrame::group_by_agg(std::vector<std::string> const &by) const {
        return factory::group_by::make_agg_by_key<DataFrame>(m_table, by);
    }

    GroupByAgg<DataFrame> DataFrame::group_by_agg(arrow::ChunkedArrayVector const &by) const {
        return factory::group_by::make_agg_by_array<DataFrame>(m_table, by);
    }

    GroupByApply DataFrame::group_by_apply(std::vector<std::string> const &by, bool groupKeys) const {
        return factory::group_by::make_apply_by_key(*this, by, groupKeys);
    }

    GroupByApply DataFrame::group_by_apply(arrow::ChunkedArrayVector const &by, bool groupKeys) const {
        return factory::group_by::make_apply_by_array(*this, by, groupKeys);
    }

    DataFrame DataFrame::apply(const std::function<Series(const Series&)>& func, AxisType axis) const {
        if (axis == AxisType::Row) {
            std::vector<FrameOrSeries> rows;
            for (int64_t i = 0; i < m_table->num_rows(); ++i) {
                rows.emplace_back(func(iloc(i)).transpose());
            }
            return concat(ConcatOptions{.frames = rows, .axis = AxisType::Row, .ignore_index = true}).reindex(m_index);
        }
        else {
            std::vector<FrameOrSeries> columns(num_cols());
            std::ranges::transform(column_names(), columns.begin(), [&](const std::string &name) {
                auto column = get_column_by_name(*m_table, name);
                return FrameOrSeries(func(Series(m_index, column, name)));
            });
            return concat(ConcatOptions{.frames = columns, .axis = AxisType::Column, .ignore_index = true}).reindex(m_index);
        }
    }

    GroupByAgg<DataFrame> DataFrame::resample_by_agg(const TimeGrouperOptions &options) const {
        return factory::group_by::make_agg_by_index<DataFrame>(*this, options);
    }

    GroupByApply DataFrame::resample_by_apply(const TimeGrouperOptions &options, bool groupKeys) const {
        return factory::group_by::make_apply_by_index(*this, groupKeys, options);
    }

    AggRollingWindowOperations<true> DataFrame::rolling_agg(window::RollingWindowOptions const& options) const {
        return {std::make_unique<window::RollingWindow>(options), *this};
    }

    ApplyDataFrameRollingWindowOperations DataFrame::rolling_apply(window::RollingWindowOptions const& options) const {
        return {std::make_unique<window::RollingWindow>(options), *this};
    }

    AggRollingWindowOperations<true> DataFrame::expanding_agg(window::ExpandingWindowOptions const& options) const {
        return {std::make_unique<window::ExpandingWindow>(options), *this};
    }

    ApplyDataFrameRollingWindowOperations DataFrame::expanding_apply(window::ExpandingWindowOptions const& options) const {
        return {std::make_unique<window::ExpandingWindow>(options), *this};
    }
} // namespace epochframe
