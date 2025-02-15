//
// Created by adesola on 2/13/25.
//

#pragma once
#include "epochframe/aliases.h"
#include <arrow/compute/api.h>

namespace epochframe {
    class Arithmetric {
    public:
        Arithmetric(TableComponent data);

        arrow::TablePtr abs() const { return apply("abs"); }

        TableComponent add(const TableComponent &otherData) const {
            return apply("add", otherData);
        }

        arrow::TablePtr add(const Scalar &other) const {
            return apply("add", other);
        }

        TableComponent divide(const TableComponent &otherData) const {
            return apply("divide", otherData);
        }

        arrow::TablePtr divide(const Scalar &other) const {
            return apply("divide", other);
        }

        arrow::TablePtr exp() const { return apply("exp"); }

        arrow::TablePtr expm1() const { return apply("expm1"); }

        TableComponent multiply(const TableComponent &otherData) const {
            return apply("multiply", otherData);
        }

        arrow::TablePtr multiply(const Scalar &other) const {
            return apply("multiply", other);
        }

        arrow::TablePtr negate() const { return apply("negate"); }

        TableComponent power(const TableComponent &otherData) const {
            return apply("power", otherData);
        }

        arrow::TablePtr power(const Scalar &other) const {
            return apply("power", other);
        }

        arrow::TablePtr sign() const { return apply("sign"); }

        arrow::TablePtr sqrt() const { return apply("sqrt"); }

        TableComponent subtract(const TableComponent &otherData) const {
            return apply("power", otherData);
        }

        arrow::TablePtr subtract(const Scalar &other) const {
            return apply("power", other);
        }

        // Bit-wise functions
        TableComponent bit_wise_and(const TableComponent &otherData) const {
            return apply("bit_wise_and", otherData);
        }

        arrow::TablePtr bit_wise_not() const { return apply("bit_wise_not"); }

        TableComponent bit_wise_or(const TableComponent &otherData) const {
            return apply("bit_wise_or", otherData);
        }

        TableComponent bit_wise_xor(const TableComponent &otherData) const {
            return apply("bit_wise_xor", otherData);
        }

        TableComponent shift_left(const TableComponent &otherData) const {
            return apply("shift_left", otherData);
        }

        TableComponent shift_right(const TableComponent &otherData) const {
            return apply("shift_right", otherData);
        }

        // Rounding Functions
        arrow::TablePtr ceil() const { return apply("ceil"); }

        arrow::TablePtr floor() const { return apply("floor"); }

        arrow::TablePtr round(arrow::compute::RoundOptions const &options) const {
            return apply("round", &options);
        }

        arrow::TablePtr round_to_multiple(arrow::compute::RoundToMultipleOptions const &options) const {
            return apply("round_to_multiple", &options);
        }

        arrow::TablePtr round_binary(arrow::compute::RoundBinaryOptions const &options) const {
            return apply("round_binary", &options);
        }

        arrow::TablePtr trunc() const { return apply("trunc"); }

        // Logarithmic Functions
        arrow::TablePtr ln() const { return apply("ln"); }

        arrow::TablePtr log10() const { return apply("log10"); }

        arrow::TablePtr log1p() const { return apply("log1p"); }

        arrow::TablePtr log2() const { return apply("log2"); }

        TableComponent logb(const TableComponent &otherData) const {
            return apply("logb", otherData);
        }

        // Trigonometric Functions
        arrow::TablePtr cos() const { return apply("cos"); }

        arrow::TablePtr sin() const { return apply("sin"); }

        arrow::TablePtr tan() const { return apply("tan"); }

        arrow::TablePtr acos() const { return apply("acos"); }

        arrow::TablePtr asin() const { return apply("asin"); }

        arrow::TablePtr atan() const { return apply("atan"); }

        TableComponent atan2(const TableComponent &otherData) const {
            return apply("atan2", otherData);
        }

        // Hyperbolic Trigonometric Functions
        arrow::TablePtr sinh() const { return apply("sinh"); }

        arrow::TablePtr cosh() const { return apply("cosh"); }

        arrow::TablePtr tanh() const { return apply("tanh"); }

        arrow::TablePtr acosh() const { return apply("acosh"); }

        arrow::TablePtr asinh() const { return apply("asinh"); }

        arrow::TablePtr atanh() const { return apply("atanh"); }

        // Cumulative
        arrow::TablePtr cumulative_sum(arrow::compute::CumulativeOptions const &option) const {
            return apply("cumulative_sum", &option);
        }

        arrow::TablePtr cumulative_prod(arrow::compute::CumulativeOptions const &option) const {
            return apply("cumulative_prod", &option);
        }

        arrow::TablePtr cumulative_max(arrow::compute::CumulativeOptions const &option) const {
            return apply("cumulative_max", &option);
        }

        arrow::TablePtr cumulative_min(arrow::compute::CumulativeOptions const &option) const {
            return apply("cumulative_min", &option);
        }

        arrow::TablePtr cumulative_mean(arrow::compute::CumulativeOptions const &option) const {
            return apply("cumulative_mean", &option);
        }

    protected:
        arrow::TablePtr apply(std::string const &op, const arrow::compute::FunctionOptions *options = nullptr) const;

        TableComponent apply(std::string const &op, const TableComponent &otherData) const;

        arrow::TablePtr apply(std::string const &op, const Scalar &other) const;

    private:
        TableComponent m_data;
    };
}
