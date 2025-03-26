//
// Created by adesola on 2/13/25.
//

#pragma once
#include "method_base.h"
#include "common/table_or_array.h"


namespace epoch_frame {
    class Arithmetic : public MethodBase {
    public:
        Arithmetic(const TableComponent& data)
                : MethodBase(data) {}

        //------------------------------------------------------------------------------
        // 1) Basic unary ops
        //------------------------------------------------------------------------------
        TableOrArray abs() const { return apply("abs"); }

        [[nodiscard]] TableOrArray negate() const { return apply("negate"); }

        [[nodiscard]] TableOrArray sign() const { return apply("sign"); }

        //------------------------------------------------------------------------------
        // 2) Basic arithmetic: + - * /, plus r* versions
        //------------------------------------------------------------------------------
        // addition
        [[nodiscard]] TableComponent add(const TableComponent &otherData) const {
            return apply("add", otherData);
        }

        [[nodiscard]] TableOrArray add(const arrow::Datum &other) const {
            return apply("add", other, /*lhs=*/true);
        }

        [[nodiscard]] TableOrArray radd(const arrow::Datum &other) const {
            return rapply("add", other);
        }

        // subtraction
        TableComponent subtract(const TableComponent &otherData) const {
            return apply("subtract", otherData);
        }

        TableOrArray subtract(const arrow::Datum &other) const {
            return apply("subtract", other, /*lhs=*/true);
        }

        TableOrArray rsubtract(const arrow::Datum &other) const {
            return rapply("subtract", other);
        }

        // multiplication
        TableComponent multiply(const TableComponent &otherData) const {
            return apply("multiply", otherData);
        }

        TableOrArray multiply(const arrow::Datum &other) const {
            return apply("multiply", other, /*lhs=*/true);
        }

        TableOrArray rmultiply(const arrow::Datum &other) const {
            return rapply("multiply", other);
        }

        // division
        TableComponent divide(const TableComponent &otherData) const {
            return apply("divide", otherData);
        }

        TableOrArray divide(const arrow::Datum &other) const {
            return apply("divide", other, /*lhs=*/true);
        }

        TableOrArray rdivide(const arrow::Datum &other) const {
            return rapply("divide", other);
        }

        //------------------------------------------------------------------------------
        // 3) Exponential, power
        //------------------------------------------------------------------------------
        TableOrArray exp() const { return apply("exp"); }

        TableOrArray expm1() const { return apply("expm1"); }

        TableComponent power(const TableComponent &otherData) const {
            return apply("power", otherData);
        }

        [[nodiscard]] TableOrArray power(const arrow::Datum &other) const {
            return apply("power", other, /*lhs=*/true);
        }

        [[nodiscard]] TableOrArray rpower(const arrow::Datum &other) const {
            return rapply("power", other);
        }

        //------------------------------------------------------------------------------
        // 4) Square roots, logs
        //------------------------------------------------------------------------------
        TableOrArray sqrt() const { return apply("sqrt"); }

        TableOrArray ln() const { return apply("ln"); }

        TableOrArray log10() const { return apply("log10"); }

        TableOrArray log1p() const { return apply("log1p"); }

        TableOrArray log2() const { return apply("log2"); }

        TableComponent logb(const TableComponent &otherData) const {
            return apply("logb", otherData);
        }

        TableOrArray rlogb(const arrow::Datum &other) const {
            return rapply("logb", other);
        }

        //------------------------------------------------------------------------------
        // 5) Bitwise ops
        //------------------------------------------------------------------------------
        TableComponent bit_wise_and(const TableComponent &otherData) const {
            return apply("bit_wise_and", otherData);
        }

        TableOrArray bit_wise_and(const arrow::Datum &other) const {
            return apply("bit_wise_and", other);
        }

        TableOrArray rbit_wise_and(const arrow::Datum &other) const {
            return rapply("bit_wise_and", other);
        }

        TableOrArray bit_wise_not() const {
            return apply("bit_wise_not");
        }

        TableComponent bit_wise_or(const TableComponent &otherData) const {
            return apply("bit_wise_or", otherData);
        }

        TableOrArray bit_wise_or(const arrow::Datum &other) const {
            return apply("bit_wise_or", other);
        }

        TableOrArray rbit_wise_or(const arrow::Datum &other) const {
            return rapply("bit_wise_or", other);
        }

        TableComponent bit_wise_xor(const TableComponent &otherData) const {
            return apply("bit_wise_xor", otherData);
        }

        TableOrArray bit_wise_xor(const arrow::Datum &other) const {
            return apply("bit_wise_xor", other);
        }

        TableOrArray rbit_wise_xor(const arrow::Datum &other) const {
            return rapply("bit_wise_xor", other);
        }

        TableComponent shift_left(const TableComponent &otherData) const {
            return apply("shift_left", otherData);
        }

        TableOrArray shift_left(const arrow::Datum &other) const {
            return apply("shift_left", other);
        }

        TableOrArray rshift_left(const arrow::Datum &other) const {
            return rapply("shift_left", other);
        }

        TableComponent shift_right(const TableComponent &otherData) const {
            return apply("shift_right", otherData);
        }

        TableOrArray shift_right(const arrow::Datum &other) const {
            return apply("shift_right", other);
        }

        TableOrArray rshift_right(const arrow::Datum &other) const {
            return rapply("shift_right", other);
        }

        //------------------------------------------------------------------------------
        // 6) Rounding
        //------------------------------------------------------------------------------
        TableOrArray ceil() const { return apply("ceil"); }

        TableOrArray floor() const { return apply("floor"); }

        TableOrArray trunc() const { return apply("trunc"); }

        TableOrArray round(const arrow::compute::RoundOptions &options) const {
            return apply("round", &options);
        }

        TableOrArray round_to_multiple(const arrow::compute::RoundToMultipleOptions &options) const {
            return apply("round_to_multiple", &options);
        }

        TableOrArray round_binary(const arrow::compute::RoundBinaryOptions &options) const {
            return apply("round_binary", &options);
        }

        //------------------------------------------------------------------------------
        // 7) Trig functions
        //------------------------------------------------------------------------------
        TableOrArray cos() const { return apply("cos"); }

        TableOrArray sin() const { return apply("sin"); }

        TableOrArray tan() const { return apply("tan"); }

        TableOrArray acos() const { return apply("acos"); }

        TableOrArray asin() const { return apply("asin"); }

        TableOrArray atan() const { return apply("atan"); }

        TableComponent atan2(const TableComponent &otherData) const {
            return apply("atan2", otherData);
        }

        TableOrArray atan2(const arrow::Datum &other) const {
            return apply("atan2", other);
        }

        TableOrArray ratan2(const arrow::Datum &other) const {
            return rapply("atan2", other);
        }

        // Hyperbolic
        TableOrArray sinh() const { return apply("sinh"); }

        TableOrArray cosh() const { return apply("cosh"); }

        TableOrArray tanh() const { return apply("tanh"); }

        TableOrArray acosh() const { return apply("acosh"); }

        TableOrArray asinh() const { return apply("asinh"); }

        TableOrArray atanh() const { return apply("atanh"); }

        //------------------------------------------------------------------------------
        // 8) Cumulative
        //------------------------------------------------------------------------------
        [[nodiscard]] TableOrArray cumulative_sum(arrow::compute::CumulativeOptions const &options) const {
            return apply("cumulative_sum", &options);
        }

        [[nodiscard]] TableOrArray cumulative_prod(arrow::compute::CumulativeOptions const &options) const {
            return apply("cumulative_prod", &options);
        }

        [[nodiscard]] TableOrArray cumulative_max(arrow::compute::CumulativeOptions const &options) const {
            return apply("cumulative_max", &options);
        }

        [[nodiscard]] TableOrArray cumulative_min(arrow::compute::CumulativeOptions const &options) const {
            return apply("cumulative_min", &options);
        }

        [[nodiscard]] TableOrArray cumulative_mean(arrow::compute::CumulativeOptions const &options) const {
            return apply("cumulative_mean", &options);
        }
    };
}
