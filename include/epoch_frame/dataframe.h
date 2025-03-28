#pragma once
#include "ndframe/ndframe.h"
#include "methods/groupby.h"
#include "methods/window.h"

// Forward declarations and type aliases
namespace epoch_frame {
    class DataFrame : public NDFrame<DataFrame, arrow::Table> {
    public:
        // ------------------------------------------------------------------------
        // Constructors / Destructor / Assignment
        // ------------------------------------------------------------------------
        DataFrame() = default;

        explicit DataFrame(arrow::TablePtr const &data);

        DataFrame(IndexPtr const &index, arrow::TablePtr const &data);

        // ------------------------------------------------------------------------
        // General Attributes
        // ------------------------------------------------------------------------
        DataFrame add_prefix(const std::string &prefix) const override {
            return add_prefix_or_suffix(prefix, true);
        }

        DataFrame add_suffix(const std::string &suffix) const override {
            return add_prefix_or_suffix(suffix, false);
        }

        DataFrame rename(std::unordered_map<std::string, std::string> const& by);

        using NDFrame<DataFrame, arrow::Table>::set_index;
        DataFrame set_index(std::string const&) const;

        //--------------------------------------------------------------------------
        // 2) Basic arithmetic: +, -, *, / with NDFrame and Scalar
        //--------------------------------------------------------------------------
        using NDFrame::operator+;
        using NDFrame::operator-;
        using NDFrame::operator*;
        using NDFrame::operator/;

        DataFrame operator+(Series const &other) const;

        DataFrame operator-(Series const &other) const;

        DataFrame operator*(Series const &other) const;

        DataFrame operator/(Series const &other) const;

        //--------------------------------------------------------------------------
        // 3) Exponential, Power, sqrt, logs, trig
        //--------------------------------------------------------------------------
        using NDFrame::power;
        using NDFrame::logb;

        DataFrame power(Series const &other) const;
        DataFrame logb(Series const &other) const;

        //--------------------------------------------------------------------------
        // 4) Bitwise ops
        //--------------------------------------------------------------------------
        using NDFrame::bitwise_and;
        using NDFrame::bitwise_or;
        using NDFrame::bitwise_xor;
        using NDFrame::shift_left;
        using NDFrame::shift_right;

        DataFrame bitwise_and(Series const &other) const;
        DataFrame bitwise_or(Series const &other) const;
        DataFrame bitwise_xor(Series const &other) const;
        DataFrame shift_left(Series const &other) const;
        DataFrame shift_right(Series const &other) const;

        //--------------------------------------------------------------------------
        // 8) Indexing ops
        //--------------------------------------------------------------------------
        using NDFrame::iloc;  // for the IntegerSlice version
        using NDFrame::loc;   // for the array/Series/callable/slice versions

        // Keep the DataFrame-specific overloads
        Series iloc(int64_t row) const;
        Scalar iloc(int64_t row, std::string const &col) const;
        Series operator[](const std::string &column) const;
        DataFrame operator[](const StringVector &columns) const;
        DataFrame operator[](const Array &array) const;
        DataFrame operator[](const StringVectorCallable &callable) const;
        Series loc(const Scalar &index_label) const;
        Scalar loc(const Scalar &index_label, const std::string &column) const;
        DataFrame loc(const DataFrameToSeriesCallable &) const;
        Series loc(const Scalar &, const LocColArgumentVariant &) const;
        DataFrame loc(const LocRowArgumentVariant &, const LocColArgumentVariant &) const;
        Series loc(const LocRowArgumentVariant &, const std::string &) const;
        DataFrame sort_columns(bool ascending=true) const;

        //--------------------------------------------------------------------------
        // 10) Comparison ops
        //--------------------------------------------------------------------------
        using NDFrame::operator==;
        using NDFrame::operator!=;
        using NDFrame::operator<;
        using NDFrame::operator<=;
        using NDFrame::operator>;
        using NDFrame::operator>=;

        //--------------------------------------------------------------------------
        // 11) Logical ops
        //--------------------------------------------------------------------------
        using NDFrame::operator&&;
        using NDFrame::operator||;
        using NDFrame::operator^;
        using NDFrame::operator!;

        //--------------------------------------------------------------------------
        // 9) Serialization
        //--------------------------------------------------------------------------
        friend std::ostream &operator<<(std::ostream &os, DataFrame const &);
        std::string repr() const;

        //--------------------------------------------------------------------------
        // 12) Common Operations
        //--------------------------------------------------------------------------
        size_t num_rows() const;

        size_t num_cols() const;

        std::vector<std::string> column_names() const;

        arrow::TablePtr table() const {
            return m_table;
        }

        Series to_series() const;

        using NDFrame::from_base;

        DataFrame from_base(IndexPtr const &index, arrow::TablePtr const &table) const override;

        DataFrame from_base(TableComponent const &tableComponent) const override;

        GroupByAgg<DataFrame> group_by_agg(std::vector<std::string> const &by) const;
        GroupByAgg<DataFrame> group_by_agg(arrow::ChunkedArrayVector const &by) const;
        GroupByApply group_by_apply(std::vector<std::string>  const &by, bool groupKeys=true) const;
        GroupByApply group_by_apply(arrow::ChunkedArrayVector const &by, bool groupKeys=true) const;

        GroupByAgg<DataFrame> resample_by_agg(const TimeGrouperOptions &options) const;
        GroupByApply resample_by_apply(const TimeGrouperOptions &options, bool groupKeys=true) const;

        GroupByAgg<DataFrame> group_by_agg(std::string const &by) const {
            return group_by_agg(std::vector<std::string>{by});
        }
        GroupByAgg<DataFrame> group_by_agg(arrow::ChunkedArrayPtr const &by) const {
            return group_by_agg(arrow::ChunkedArrayVector{by});
        }
        GroupByApply group_by_apply(std::string  const &by, bool groupKeys=true) const {
            return group_by_apply(std::vector<std::string>{by}, groupKeys);
        }
        GroupByApply group_by_apply(arrow::ChunkedArrayPtr const &by, bool groupKeys=true) const {
            return group_by_apply(arrow::ChunkedArrayVector{by}, groupKeys);
        }

        /**
         * @brief Apply a function to each row/column of the DataFrame
         *
         * @param func A function that takes a row/column as a Series and returns a Series
         * @param axis The axis to apply the function to. If AxisType::Row, the function is applied to each row. If AxisType::Column, the function is applied to each column.
         * @return A new DataFrame with the results
         */
        DataFrame apply(const std::function<Series(const Series&)>& func, AxisType axis = AxisType::Row) const;
        DataFrame apply(const std::function<Array(const Array&)>& func, AxisType axis = AxisType::Row) const;
        AggRollingWindowOperations<true> rolling_agg(window::RollingWindowOptions const& options) const;
        ApplyDataFrameRollingWindowOperations rolling_apply(window::RollingWindowOptions const& options) const;

        AggRollingWindowOperations<true> expanding_agg(window::ExpandingWindowOptions const& options) const;
        ApplyDataFrameRollingWindowOperations expanding_apply(window::ExpandingWindowOptions const& options) const;


        // Assignments
        DataFrame assign(std::string const&, Series const&) const;

        DataFrame assign(IndexPtr const&, arrow::TablePtr const&) const;
        DataFrame assign(DataFrame const& df) const{
            return assign(df.m_index, df.m_table);
        }

        DataFrame drop(std::string const& column) const;
        DataFrame drop(std::vector<std::string> const& columns) const;
        bool contains(std::string const& column) const{
            return m_table->schema()->GetFieldIndex(column) != -1;
        }

        // Inherit map method from NDFrame
        using NDFrame::map;
    private:
        DataFrame add_prefix_or_suffix(const std::string &prefix_or_suffix, bool is_prefix) const;
    };
} // namespace epoch_frame
