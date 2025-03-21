//
// Created by adesola on 2/16/25.
//

#pragma once
#include <factory/array_factory.h>
#include <methods/temporal.h>
#include <methods/string.h>
#include "ndframe/ndframe.h"
#include "methods/groupby.h"
#include "methods/window.h"

namespace epochframe {
    class Series : public NDFrame<Series, arrow::ChunkedArray> {

    public:
        // ------------------------------------------------------------------------
        // Constructors / Destructor / Assignment
        // ------------------------------------------------------------------------
        Series();

        Series(arrow::ChunkedArrayPtr const &data, std::optional<std::string> const &name = {});
        Series(arrow::ArrayPtr const &data, std::optional<std::string> const &name = {});
        Series(arrow::ScalarPtr const &data, IndexPtr const& index, std::optional<std::string> const &name={});
        Series(IndexPtr index, arrow::ChunkedArrayPtr data, std::optional<std::string> const &name = {});
        Series(IndexPtr index, arrow::ArrayPtr data, std::optional<std::string> const &name = {});

        // ------------------------------------------------------------------------
        // General Attributes
        // ------------------------------------------------------------------------

        Series add_prefix(const std::string &prefix) const override {
            return Series(m_table, prefix + m_name.value_or(""));
        }

        Series add_suffix(const std::string &suffix) const override {
            return Series(m_table, m_name.value_or("") + suffix);
        }

        DataFrame to_frame(std::optional<std::string> const &name=std::nullopt) const;
        DataFrame transpose() const;

        std::optional<std::string> name() const {
            return m_name;
        }

        arrow::ChunkedArrayPtr array() const {
            return m_table;
        }

        Array contiguous_array() const {
            return Array(factory::array::make_contiguous_array(m_table));
        }

        friend std::ostream &operator<<(std::ostream &os, Series const &series);

        //--------------------------------------------------------------------------
        // 2) Basic arithmetic: +, -, *, / with NDFrame and Scalar
        //--------------------------------------------------------------------------
        using NDFrame::operator+;
        using NDFrame::operator-;
        using NDFrame::operator*;
        using NDFrame::operator/;

        DataFrame operator+(DataFrame const &other) const;

        DataFrame operator-(DataFrame const &other) const;

        DataFrame operator*(DataFrame const &other) const;

        DataFrame operator/(DataFrame const &other) const;

        //--------------------------------------------------------------------------
        // 3) Exponential, Power, sqrt, logs, trig
        //--------------------------------------------------------------------------
        using NDFrame::power;
        using NDFrame::logb;

        DataFrame power(DataFrame const &other) const;

        DataFrame logb(DataFrame const &other) const;

        //--------------------------------------------------------------------------
        // 4) Bitwise ops
        //--------------------------------------------------------------------------
        using NDFrame::bitwise_and;
        using NDFrame::bitwise_or;
        using NDFrame::bitwise_xor;
        using NDFrame::shift_left;
        using NDFrame::shift_right;

        DataFrame bitwise_and(DataFrame const &other) const;

        DataFrame bitwise_or(DataFrame const &other) const;

        DataFrame bitwise_xor(DataFrame const &other) const;

        DataFrame shift_left(DataFrame const &other) const;

        DataFrame shift_right(DataFrame const &other) const;


        //--------------------------------------------------------------------------
        // 8) Indexing ops
        //--------------------------------------------------------------------------
        using NDFrame::iloc;
        using NDFrame::loc;

        Scalar iloc(int64_t row) const;
        Scalar loc(const Scalar &index_label) const;
        Series loc(const SeriesToSeriesCallable &) const;

        //--------------------------------------------------------------------------
        // 13) Selection & Transform
        //--------------------------------------------------------------------------
        arrow::ArrayPtr unique() const;

        [[nodiscard]] TemporalOperation<true> dt() const {
            return TemporalOperation<true>(Array(factory::array::make_contiguous_array(m_table)));
        }
        
        /**
         * Return a StringOperation object that can be used to call string methods on this Series.
         * @return StringOperation
         */
        [[nodiscard]] StringOperation<true> str() const {
            return StringOperation<true>(Array(factory::array::make_contiguous_array(m_table)));
        }
        

        GroupByAgg<Series> resample_by_agg(const TimeGrouperOptions &options) const;
        GroupByApply resample_by_apply(const TimeGrouperOptions &options, bool groupKeys=true) const;
                
        AggRollingWindowOperations<false> rolling_agg(window::RollingWindowOptions const& options) const;
        ApplySeriesRollingWindowOperations rolling_apply(window::RollingWindowOptions const& options) const;

        AggRollingWindowOperations<false> expanding_agg(window::ExpandingWindowOptions const& options) const;
        ApplySeriesRollingWindowOperations expanding_apply(window::ExpandingWindowOptions const& options) const;

        Series diff(int64_t periods=1) const;

        Series shift(int64_t periods=1) const;

        Series pct_change(int64_t periods=1) const;

        Scalar cov(Series const &other, int64_t min_periods=1, int64_t ddof=1) const;

        Scalar corr(Series const &other, int64_t min_periods=1, int64_t ddof=1) const;  

        using NDFrame::from_base;

    private:
        std::optional<std::string> m_name;

        Series from_base(IndexPtr const &index, ArrowPtrType const &table) const override;

        Series from_base(TableComponent const &tableComponent) const override;
    };
}
