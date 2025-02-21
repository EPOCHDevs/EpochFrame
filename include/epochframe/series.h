//
// Created by adesola on 2/16/25.
//

#pragma once
#include "aliases.h"
#include <optional>
#include <arrow/chunked_array.h>
#include "ndframe/ndframe.h"
#include "index/index.h"
#include "common/table_or_array.h"


namespace epochframe {
    class Series : public NDFrame<Series, arrow::ChunkedArray> {

    public:
        // Constructors / Destructor / Assignment
        Series();

        Series(arrow::ChunkedArrayPtr const &data, std::optional<std::string> const &name = {});
        Series(arrow::ArrayPtr const &data, std::optional<std::string> const &name = {});
        Series(IndexPtr index, arrow::ChunkedArrayPtr data, std::optional<std::string> const &name = {});
        Series(IndexPtr index, arrow::ArrayPtr data, std::optional<std::string> const &name = {});
        // General Attributes

        Series add_prefix(const std::string &prefix) const override {
            return Series(m_table, prefix + m_name.value_or(""));
        }

        Series add_suffix(const std::string &suffix) const override {
            return Series(m_table, m_name.value_or("") + suffix);
        }

        bool equals(Series const& x) const {
            return m_index->equals(x.m_index) && m_table->Equals(x.m_table);
        }

        class DataFrame to_frame(std::optional<std::string> const &name=std::nullopt) const;

        std::optional<std::string> name() const {
            return m_name;
        }

        Scalar operator[](int64_t i) const;

        Scalar loc(Scalar const&) const;

        arrow::ChunkedArrayPtr array() const {
            return m_table;
        }

        friend std::ostream &operator<<(std::ostream &os, Series const &series);

//        std::shared_ptr<arrow::BooleanArray>
//        duplicated(DropDuplicatesKeepPolicy keep = DropDuplicatesKeepPolicy::First) const final;
//
//        std::shared_ptr<Index>
//        drop_duplicates(DropDuplicatesKeepPolicy keep = DropDuplicatesKeepPolicy::First) const final;
//        virtual std::shared_ptr<Index> putmask(arrow::ArrayPtr const &mask,
//                                               arrow::ArrayPtr const &other) const = 0;


        //--------------------------------------------------------------------------
        // 2) Basic arithmetic: +, -, *, / with NDFrame and Scalar
        //--------------------------------------------------------------------------
        using NDFrame::operator+;
        using NDFrame::operator-;
        using NDFrame::operator*;
        using NDFrame::operator/;
        using NDFrame::power;
        using NDFrame::logb;
        using NDFrame::bitwise_and;
        using NDFrame::bitwise_or;
        using NDFrame::bitwise_xor;
        using NDFrame::shift_left;
        using NDFrame::shift_right;

        class DataFrame operator+(class DataFrame const &other) const;

        DataFrame operator-(DataFrame const &other) const;

        DataFrame operator*(DataFrame const &other) const;

        DataFrame operator/(DataFrame const &other) const;

        //--------------------------------------------------------------------------
        // 3) Exponential, Power, sqrt, logs, trig
        //--------------------------------------------------------------------------

        DataFrame power(DataFrame const &other) const;

        DataFrame logb(DataFrame const &other) const;

        //--------------------------------------------------------------------------
        // 4) Bitwise ops
        //--------------------------------------------------------------------------
        DataFrame bitwise_and(DataFrame const &other) const;

        DataFrame bitwise_or(DataFrame const &other) const;

        DataFrame bitwise_xor(DataFrame const &other) const;

        DataFrame shift_left(DataFrame const &other) const;

        DataFrame shift_right(DataFrame const &other) const;


        //--------------------------------------------------------------------------
        // 8) Indexing ops
        //--------------------------------------------------------------------------

        using NDFrame::from_base;

    private:
        std::optional<std::string> m_name;

        Series from_base(IndexPtr const &index, ArrowPtrType const &table) const override {
            return Series(index, table, m_name);
        }

        Series from_base(TableComponent const &tableComponent) const override {
            return Series(tableComponent.first, tableComponent.second.chunked_array(), m_name);
        }
    };
}
