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

        arrow::RecordBatchPtr abs() const { return apply("abs"); }

        arrow::RecordBatchPtr add(const TableComponent &otherData) const {
            return apply("add", otherData);
        }

        arrow::RecordBatchPtr add(const Scalar &other) const {
            return apply("add", other);
        }

        arrow::RecordBatchPtr divide(const TableComponent &otherData) const {
            return apply("divide", otherData);
        }

        arrow::RecordBatchPtr divide(const Scalar &other) const {
            return apply("divide", other);
        }

        arrow::RecordBatchPtr exp() const { return apply("exp"); }

        arrow::RecordBatchPtr expm1() const { return apply("expm1"); }

        arrow::RecordBatchPtr multiply(const TableComponent &otherData) const {
            return apply("multiply", otherData);
        }

        arrow::RecordBatchPtr multiply(const Scalar &other) const {
            return apply("multiply", other);
        }

        arrow::RecordBatchPtr negate() const { return apply("negate"); }

        arrow::RecordBatchPtr power(const TableComponent &otherData) const {
            return apply("power", otherData);
        }

        arrow::RecordBatchPtr power(const Scalar &other) const {
            return apply("power", other);
        }

        arrow::RecordBatchPtr sign() const { return apply("sign"); }

        arrow::RecordBatchPtr sqrt() const { return apply("sqrt"); }

        arrow::RecordBatchPtr subtract(const TableComponent &otherData) const {
            return apply("power", otherData);
        }

        arrow::RecordBatchPtr subtract(const Scalar &other) const {
            return apply("power", other);
        }

        // Bit-wise functions
        arrow::RecordBatchPtr bit_wise_and(const TableComponent &otherData) const {
            return apply("bit_wise_and", otherData);
        }

        arrow::RecordBatchPtr bit_wise_not() const { return apply("bit_wise_not"); }

        arrow::RecordBatchPtr bit_wise_or(const TableComponent &otherData) const {
            return apply("bit_wise_or", otherData);
        }

        arrow::RecordBatchPtr bit_wise_xor(const TableComponent &otherData) const {
            return apply("bit_wise_xor", otherData);
        }

        arrow::RecordBatchPtr shift_left(const TableComponent &otherData) const {
            return apply("shift_left", otherData);
        }

        arrow::RecordBatchPtr shift_right(const TableComponent &otherData) const {
            return apply("shift_right", otherData);
        }

        // Rounding Functions
        arrow::RecordBatchPtr ceil() const { return apply("ceil"); }

        arrow::RecordBatchPtr floor() const { return apply("floor"); }

        arrow::RecordBatchPtr round(arrow::compute::RoundOptions const &options) const {
            return apply("round", options);
        }

        arrow::RecordBatchPtr round_to_multiple(arrow::compute::RoundToMultipleOptions const &options) const {
            return apply("round_to_multiple", options);
        }

        arrow::RecordBatchPtr round_binary(arrow::compute::RoundBinaryOptions const &options) const {
            return apply("round_binary", options);
        }

        arrow::RecordBatchPtr trunc() const { return apply("trunc"); }

        // Logarithmic Functions
        arrow::RecordBatchPtr ln() const { return apply("ln"); }

        arrow::RecordBatchPtr log10() const { return apply("log10"); }

        arrow::RecordBatchPtr log1p() const { return apply("log1p"); }

        arrow::RecordBatchPtr log2() const { return apply("log2"); }

        arrow::RecordBatchPtr logb(const TableComponent &otherData) const {
            return apply("logb", otherData);
        }

        // Trigonometric Functions
        arrow::RecordBatchPtr cos() const { return apply("cos"); }

        arrow::RecordBatchPtr sin() const { return apply("sin"); }

        arrow::RecordBatchPtr tan() const { return apply("tan"); }

        arrow::RecordBatchPtr acos() const { return apply("acos"); }

        arrow::RecordBatchPtr asin() const { return apply("asin"); }

        arrow::RecordBatchPtr atan() const { return apply("atan"); }

        arrow::RecordBatchPtr atan2(const TableComponent &otherData) const {
            return apply("atan2", otherData);
        }

        // Hyperbolic Trigonometric Functions
        arrow::RecordBatchPtr sinh() const { return apply("sinh"); }

        arrow::RecordBatchPtr cosh() const { return apply("cosh"); }

        arrow::RecordBatchPtr tanh() const { return apply("tanh"); }

        arrow::RecordBatchPtr acosh() const { return apply("acosh"); }

        arrow::RecordBatchPtr asinh() const { return apply("asinh"); }

        arrow::RecordBatchPtr atanh() const { return apply("atanh"); }

        // Cumulative
        arrow::RecordBatchPtr cumulative_sum(arrow::compute::CumulativeOptions const &option) const {
            return apply("cumulative_sum", option);
        }

        arrow::RecordBatchPtr cumulative_prod(arrow::compute::CumulativeOptions const &option) const {
            return apply("cumulative_prod", option);
        }

        arrow::RecordBatchPtr cumulative_max(arrow::compute::CumulativeOptions const &option) const {
            return apply("cumulative_max", option);
        }

        arrow::RecordBatchPtr cumulative_min(arrow::compute::CumulativeOptions const &option) const {
            return apply("cumulative_min", option);
        }

        arrow::RecordBatchPtr cumulative_mean(arrow::compute::CumulativeOptions const &option) const {
            return apply("cumulative_mean", option);
        }

    private:
        TableComponent m_data;

        arrow::RecordBatchPtr apply(std::string const &op) const;

        arrow::RecordBatchPtr apply(std::string const &op, const arrow::compute::FunctionOptions &) const;

        arrow::RecordBatchPtr apply(std::string const &op, const TableComponent &otherData) const;

        arrow::RecordBatchPtr apply(std::string const &op, const Scalar &other) const;
    };
}
