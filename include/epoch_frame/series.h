//
// Created by adesola on 2/16/25.
//

#pragma once
#include "epoch_frame/factory/array_factory.h"
#include "epoch_frame/scalar.h"
#include "methods/groupby.h"
#include "methods/string.h"
#include "methods/temporal.h"
#include "methods/window.h"
#include "ndframe/ndframe.h"

namespace epoch_frame
{
    class Series : public NDFrame<Series, arrow::ChunkedArray>
    {

      public:
        // ------------------------------------------------------------------------
        // Constructors / Destructor / Assignment
        // ------------------------------------------------------------------------
        Series();

        explicit Series(arrow::ChunkedArrayPtr const&     data,
                        std::optional<std::string> const& name = {});
        explicit Series(arrow::ArrayPtr const& data, std::optional<std::string> const& name = {});
        Series(arrow::ScalarPtr const& data, IndexPtr const& index,
               std::optional<std::string> const& name = {});
        Series(IndexPtr index, arrow::ChunkedArrayPtr data,
               std::optional<std::string> const& name = {});
        Series(IndexPtr index, arrow::ArrayPtr data, std::optional<std::string> const& name = {});

        // ------------------------------------------------------------------------
        // General Attributes
        // ------------------------------------------------------------------------

        arrow::DataTypePtr dtype() const
        {
            return m_table->type();
        }

        Series add_prefix(const std::string& prefix) const override
        {
            return Series(m_table, prefix + m_name.value_or(""));
        }

        Series add_suffix(const std::string& suffix) const override
        {
            return Series(m_table, m_name.value_or("") + suffix);
        }

        Series rename(std::string const& name) const
        {
            return Series(m_index, m_table, name);
        }

        Series n_largest(int64_t n) const;
        Series n_smallest(int64_t n) const;

        DataFrame to_frame(std::optional<std::string> const& name = std::nullopt) const;
        DataFrame transpose(IndexPtr const& new_index) const;

        std::optional<std::string> name() const
        {
            return m_name;
        }

        arrow::ChunkedArrayPtr array() const
        {
            return m_table;
        }

        Array contiguous_array() const
        {
            return Array(factory::array::make_contiguous_array(m_table));
        }

        friend std::ostream& operator<<(std::ostream& os, Series const& series);
        std::string          repr() const;
        //--------------------------------------------------------------------------
        // 2) Basic arithmetic: +, -, *, / with NDFrame and Scalar
        //--------------------------------------------------------------------------
        using NDFrame::operator+;
        using NDFrame::operator-;
        using NDFrame::operator*;
        using NDFrame::operator/;

        DataFrame operator+(DataFrame const& other) const;

        DataFrame operator-(DataFrame const& other) const;

        DataFrame operator*(DataFrame const& other) const;

        DataFrame operator/(DataFrame const& other) const;

        //--------------------------------------------------------------------------
        // 3) Exponential, Power, sqrt, logs, trig
        //--------------------------------------------------------------------------
        using NDFrame::logb;
        using NDFrame::power;

        DataFrame power(DataFrame const& other) const;

        DataFrame logb(DataFrame const& other) const;

        //--------------------------------------------------------------------------
        // 4) Bitwise ops
        //--------------------------------------------------------------------------
        using NDFrame::bitwise_and;
        using NDFrame::bitwise_or;
        using NDFrame::bitwise_xor;
        using NDFrame::shift_left;
        using NDFrame::shift_right;

        DataFrame bitwise_and(DataFrame const& other) const;

        DataFrame bitwise_or(DataFrame const& other) const;

        DataFrame bitwise_xor(DataFrame const& other) const;

        DataFrame shift_left(DataFrame const& other) const;

        DataFrame shift_right(DataFrame const& other) const;

        //--------------------------------------------------------------------------
        // 8) Indexing ops
        //--------------------------------------------------------------------------
        using NDFrame::iloc;
        using NDFrame::loc;

        Scalar iloc(int64_t row) const;
        Scalar loc(const Scalar& index_label) const;
        Series safe_loc(const Scalar& index_label) const;
        Series loc(const SeriesToSeriesCallable&) const;

        //--------------------------------------------------------------------------
        // 13) Selection & Transform
        //--------------------------------------------------------------------------
        arrow::ArrayPtr unique() const;
        bool
        is_approx_equal(const Series&              other,
                        arrow::EqualOptions const& options = arrow::EqualOptions::Defaults()) const
        {
            return m_table->ApproxEquals(*other.m_table, options);
        }

        [[nodiscard]] TemporalOperation<true> dt() const
        {
            return TemporalOperation<true>(Array(factory::array::make_contiguous_array(m_table)));
        }

        Scalar idx_min() const;

        Scalar idx_max() const;

        /**
         * Return a StringOperation object that can be used to call string methods on this Series.
         * @return StringOperation
         */
        [[nodiscard]] StringOperation<true> str() const
        {
            return StringOperation<true>(Array(factory::array::make_contiguous_array(m_table)));
        }

        GroupByAgg<Series> resample_by_agg(const TimeGrouperOptions& options) const;
        GroupByApply       resample_by_apply(const TimeGrouperOptions& options,
                                             bool                      groupKeys = true) const;

        AggRollingWindowOperations<false>
        rolling_agg(window::RollingWindowOptions const& options) const;
        ApplySeriesRollingWindowOperations
        rolling_apply(window::RollingWindowOptions const& options) const;

        AggRollingWindowOperations<false>
        expanding_agg(window::ExpandingWindowOptions const& options) const;
        ApplySeriesRollingWindowOperations
        expanding_apply(window::ExpandingWindowOptions const& options) const;

        Series diff(int64_t periods = 1) const;

        Series shift(int64_t periods = 1) const;

        Series pct_change(int64_t periods = 1) const;

        Scalar cov(Series const& other, int64_t min_periods = 1, int64_t ddof = 1) const;

        Scalar corr(Series const& other, int64_t min_periods = 1, int64_t ddof = 1) const;

        Series assign(IndexPtr const&, arrow::ChunkedArrayPtr const&) const;
        Series assign(Series const& s) const
        {
            return assign(s.index(), s.array());
        }

        using NDFrame::drop;
        using NDFrame::drop_null;
        using NDFrame::from_base;

      private:
        std::optional<std::string> m_name;

        Series from_base(IndexPtr const& index, ArrowPtrType const& table) const override;

        Series from_base(TableComponent const& tableComponent) const override;
    };
} // namespace epoch_frame
