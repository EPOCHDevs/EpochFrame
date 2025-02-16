//
// Created by adesola on 2/13/25.
//

#pragma once
#include "method_base.h"


namespace epochframe {
    class Arithmetic : public MethodBase {
    public:
        Arithmetic(TableComponent data)
                : MethodBase(std::move(data)) {}

        //------------------------------------------------------------------------------
        // 1) Basic unary ops
        //------------------------------------------------------------------------------
        arrow::TablePtr abs()    const { return apply("abs"); }
        [[nodiscard]] arrow::TablePtr negate() const { return apply("negate"); }
        [[nodiscard]] arrow::TablePtr sign()   const { return apply("sign"); }

        //------------------------------------------------------------------------------
        // 2) Basic arithmetic: + - * /, plus r* versions
        //------------------------------------------------------------------------------
        // addition
        [[nodiscard]] TableComponent add(const TableComponent &otherData) const {
            return apply("add", otherData);
        }
        [[nodiscard]] arrow::TablePtr add(const Scalar &other) const {
            return apply("add", other, /*lhs=*/true);
        }
        [[nodiscard]] arrow::TablePtr radd(const Scalar &other) const {
            return rapply("add", other);
        }

        // subtraction
        TableComponent subtract(const TableComponent &otherData) const {
            return apply("subtract", otherData);
        }
        arrow::TablePtr subtract(const Scalar &other) const {
            return apply("subtract", other, /*lhs=*/true);
        }
        arrow::TablePtr rsubtract(const Scalar &other) const {
            return rapply("subtract", other);
        }

        // multiplication
        TableComponent multiply(const TableComponent &otherData) const {
            return apply("multiply", otherData);
        }
        arrow::TablePtr multiply(const Scalar &other) const {
            return apply("multiply", other, /*lhs=*/true);
        }
        arrow::TablePtr rmultiply(const Scalar &other) const {
            return rapply("multiply", other);
        }

        // division
        TableComponent divide(const TableComponent &otherData) const {
            return apply("divide", otherData);
        }
        arrow::TablePtr divide(const Scalar &other) const {
            return apply("divide", other, /*lhs=*/true);
        }
        arrow::TablePtr rdivide(const Scalar &other) const {
            return rapply("divide", other);
        }

        //------------------------------------------------------------------------------
        // 3) Exponential, power
        //------------------------------------------------------------------------------
        arrow::TablePtr exp()    const { return apply("exp"); }
        arrow::TablePtr expm1()  const { return apply("expm1"); }

        TableComponent power(const TableComponent &otherData) const {
            return apply("power", otherData);
        }
        arrow::TablePtr power(const Scalar &other) const {
            return apply("power", other, /*lhs=*/true);
        }
        arrow::TablePtr rpower(const Scalar &other) const {
            return rapply("power", other);
        }

        //------------------------------------------------------------------------------
        // 4) Square roots, logs
        //------------------------------------------------------------------------------
        arrow::TablePtr sqrt()  const { return apply("sqrt"); }
        arrow::TablePtr ln()    const { return apply("ln"); }
        arrow::TablePtr log10() const { return apply("log10"); }
        arrow::TablePtr log1p() const { return apply("log1p"); }
        arrow::TablePtr log2()  const { return apply("log2"); }

        // NDFrame op NDFrame
        TableComponent logb(const TableComponent &otherData) const {
            return apply("logb", otherData);
        }

        //------------------------------------------------------------------------------
        // 5) Bitwise ops
        //------------------------------------------------------------------------------
        TableComponent bit_wise_and(const TableComponent &otherData) const {
            return apply("bit_wise_and", otherData);
        }
        arrow::TablePtr bit_wise_not() const {
            return apply("bit_wise_not");
        }
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

        //------------------------------------------------------------------------------
        // 6) Rounding
        //------------------------------------------------------------------------------
        arrow::TablePtr ceil()  const { return apply("ceil"); }
        arrow::TablePtr floor() const { return apply("floor"); }
        arrow::TablePtr trunc() const { return apply("trunc"); }

        arrow::TablePtr round(const arrow::compute::RoundOptions &options) const {
            return apply("round", &options);
        }
        arrow::TablePtr round_to_multiple(const arrow::compute::RoundToMultipleOptions &options) const {
            return apply("round_to_multiple", &options);
        }
        arrow::TablePtr round_binary(const arrow::compute::RoundBinaryOptions &options) const {
            return apply("round_binary", &options);
        }

        //------------------------------------------------------------------------------
        // 7) Trig functions
        //------------------------------------------------------------------------------
        arrow::TablePtr cos()  const { return apply("cos"); }
        arrow::TablePtr sin()  const { return apply("sin"); }
        arrow::TablePtr tan()  const { return apply("tan"); }
        arrow::TablePtr acos() const { return apply("acos"); }
        arrow::TablePtr asin() const { return apply("asin"); }
        arrow::TablePtr atan() const { return apply("atan"); }

        TableComponent atan2(const TableComponent &otherData) const {
            return apply("atan2", otherData);
        }

        // Hyperbolic
        arrow::TablePtr sinh()  const { return apply("sinh"); }
        arrow::TablePtr cosh()  const { return apply("cosh"); }
        arrow::TablePtr tanh()  const { return apply("tanh"); }
        arrow::TablePtr acosh() const { return apply("acosh"); }
        arrow::TablePtr asinh() const { return apply("asinh"); }
        arrow::TablePtr atanh() const { return apply("atanh"); }

        //------------------------------------------------------------------------------
        // 8) Cumulative
        //------------------------------------------------------------------------------
        [[nodiscard]] arrow::TablePtr cumulative_sum(arrow::compute::CumulativeOptions const &options) const {
            return apply("cumulative_sum", &options);
        }
        [[nodiscard]] arrow::TablePtr cumulative_prod(arrow::compute::CumulativeOptions const &options) const {
            return apply("cumulative_prod", &options);
        }
        [[nodiscard]] arrow::TablePtr cumulative_max(arrow::compute::CumulativeOptions const &options) const {
            return apply("cumulative_max", &options);
        }
        [[nodiscard]] arrow::TablePtr cumulative_min(arrow::compute::CumulativeOptions const &options) const {
            return apply("cumulative_min", &options);
        }
        arrow::TablePtr cumulative_mean(arrow::compute::CumulativeOptions const &options) const {
            return apply("cumulative_mean", &options);
        }
    };
}
