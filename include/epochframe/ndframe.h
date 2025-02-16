//
// Created by adesola on 2/13/25.
//

#pragma once
#include "epochframe/scalar.h"
#include "methods/arith.h"
#include "methods/compare.h"
#include "methods/common_op.h"


namespace epochframe {
    class NDFrame {

    public:
        NDFrame();

        explicit NDFrame(arrow::TablePtr const &data);

        NDFrame(IndexPtr const &index, arrow::TablePtr const &data);

        NDFrame(TableComponent const &tableComponent) : NDFrame(tableComponent.first, tableComponent.second) {}

        NDFrame(NDFrame const &other) = default;

        NDFrame(NDFrame &&other) = default;

        NDFrame &operator=(NDFrame const &other) = default;

        NDFrame &operator=(NDFrame &&other) = default;

        virtual ~NDFrame() = default;

        // General Attributes
        void add_prefix(const std::string &prefix);

        void add_suffix(const std::string &suffix);

        // Indexing Operation
        NDFrame head(uint64_t n = 10) const;

        NDFrame tail(uint64_t n = 10) const;

        NDFrame loc(uint64_t index_label) const;

        NDFrame loc(const Scalar &index_label) const;

        NDFrame loc(const std::vector<bool> &filter) const;

        NDFrame loc(const std::vector<uint64_t> &labels) const;

        NDFrame loc(const arrow::Array &labels) const;

        NDFrame loc(const IntgerSliceType &labels) const;

        NDFrame loc(const SliceType &labels) const;

        Scalar at(uint64_t index_label) const;

        Scalar at(const Scalar &index_label, const std::string &column) const;

        Scalar at(const Scalar &index_label) const;

        NDFrame at(const std::string &column) const;

        IndexPtr index() const;

        void set_index(IndexPtr const &index);

        // General Attributes
        [[nodiscard]] Shape2D shape() const;

        [[nodiscard]] bool empty() const;

        [[nodiscard]] uint64_t size() const;

        //--------------------------------------------------------------------------
        // 1) Basic unary ops
        //--------------------------------------------------------------------------
        NDFrame abs() const { return NDFrame{m_data.first, m_arithOp->abs()}; }

        NDFrame operator-() const { return NDFrame{m_data.first, m_arithOp->negate()}; }

        NDFrame sign() const { return NDFrame{m_data.first, m_arithOp->sign()}; }

        //--------------------------------------------------------------------------
        // 2) Basic arithmetic: +, -, *, / with NDFrame and Scalar
        //--------------------------------------------------------------------------
        // + NDFrame
        NDFrame operator+(NDFrame const &other) const {
            return NDFrame{m_arithOp->add(other.m_data)};
        }

        // + Scalar
        NDFrame operator+(Scalar const &val) const {
            return NDFrame{m_data.first, m_arithOp->add(val)};
        }

        // Scalar + NDFrame
        friend NDFrame operator+(Scalar const &lhs, NDFrame const &rhs) {
            return NDFrame{rhs.m_data.first, rhs.m_arithOp->radd(lhs)};
        }

        // - NDFrame
        NDFrame operator-(NDFrame const &other) const {
            return NDFrame{m_arithOp->subtract(other.m_data)};
        }

        // - Scalar
        NDFrame operator-(Scalar const &val) const {
            return NDFrame{m_data.first, m_arithOp->subtract(val)};
        }

        // Scalar - NDFrame
        friend NDFrame operator-(Scalar const &lhs, NDFrame const &rhs) {
            return NDFrame{rhs.m_data.first, rhs.m_arithOp->rsubtract(lhs)};
        }

        // * NDFrame
        NDFrame operator*(NDFrame const &other) const {
            return NDFrame{m_arithOp->multiply(other.m_data)};
        }

        // * Scalar
        NDFrame operator*(Scalar const &val) const {
            return NDFrame{m_data.first, m_arithOp->multiply(val)};
        }

        // Scalar * NDFrame
        friend NDFrame operator*(Scalar const &lhs, NDFrame const &rhs) {
            return NDFrame{rhs.m_data.first, rhs.m_arithOp->rmultiply(lhs)};
        }

        // / NDFrame
        NDFrame operator/(NDFrame const &other) const {
            return NDFrame{m_arithOp->divide(other.m_data)};
        }

        // / Scalar
        NDFrame operator/(Scalar const &val) const {
            return NDFrame{m_data.first, m_arithOp->divide(val)};
        }

        // Scalar / NDFrame
        friend NDFrame operator/(Scalar const &lhs, NDFrame const &rhs) {
            return NDFrame{rhs.m_data.first, rhs.m_arithOp->rdivide(lhs)};
        }

        //--------------------------------------------------------------------------
        // 3) Exponential, Power
        //--------------------------------------------------------------------------
        NDFrame exp() const { return NDFrame{m_data.first, m_arithOp->exp()}; }

        NDFrame expm1() const { return NDFrame{m_data.first, m_arithOp->expm1()}; }

        // NDFrame^NDFrame (power)
        NDFrame power(NDFrame const &other) const {
            return NDFrame{m_arithOp->power(other.m_data)};
        }

        // NDFrame^Scalar
        NDFrame power(Scalar const &val) const {
            return NDFrame{m_data.first, m_arithOp->power(val)};
        }

        // Scalar^NDFrame
        friend NDFrame power(Scalar const &lhs, NDFrame const &rhs) {
            return NDFrame{rhs.m_data.first, rhs.m_arithOp->rpower(lhs)};
        }

        //--------------------------------------------------------------------------
        // 4) sqrt, logs
        //--------------------------------------------------------------------------
        NDFrame sqrt() const { return NDFrame{m_data.first, m_arithOp->sqrt()}; }

        NDFrame ln() const { return NDFrame{m_data.first, m_arithOp->ln()}; }

        NDFrame log10() const { return NDFrame{m_data.first, m_arithOp->log10()}; }

        NDFrame log1p() const { return NDFrame{m_data.first, m_arithOp->log1p()}; }

        NDFrame log2() const { return NDFrame{m_data.first, m_arithOp->log2()}; }

        // logb with NDFrame
        NDFrame logb(NDFrame const &other) const {
            return NDFrame{m_arithOp->logb(other.m_data)};
        }

        //--------------------------------------------------------------------------
        // 5) Bitwise ops
        //--------------------------------------------------------------------------
        NDFrame bitwise_and(NDFrame const &other) const {
            return NDFrame{m_arithOp->bit_wise_and(other.m_data)};
        }

        NDFrame bitwise_not() const {
            return NDFrame{m_data.first, m_arithOp->bit_wise_not()};
        }

        NDFrame bitwise_or(NDFrame const &other) const {
            return NDFrame{m_arithOp->bit_wise_or(other.m_data)};
        }

        NDFrame bitwise_xor(NDFrame const &other) const {
            return NDFrame{m_arithOp->bit_wise_xor(other.m_data)};
        }

        NDFrame shift_left(NDFrame const &other) const {
            return NDFrame{m_arithOp->shift_left(other.m_data)};
        }

        NDFrame shift_right(NDFrame const &other) const {
            return NDFrame{m_arithOp->shift_right(other.m_data)};
        }

        //--------------------------------------------------------------------------
        // 6) Rounding
        //--------------------------------------------------------------------------
        NDFrame ceil() const { return NDFrame{m_data.first, m_arithOp->ceil()}; }

        NDFrame floor() const { return NDFrame{m_data.first, m_arithOp->floor()}; }

        NDFrame trunc() const { return NDFrame{m_data.first, m_arithOp->trunc()}; }

        NDFrame round(arrow::compute::RoundOptions const &options) const {
            return NDFrame{m_data.first, m_arithOp->round(options)};
        }

        NDFrame round_to_multiple(arrow::compute::RoundToMultipleOptions const &options) const {
            return NDFrame{m_data.first, m_arithOp->round_to_multiple(options)};
        }

        NDFrame round_binary(arrow::compute::RoundBinaryOptions const &options) const {
            return NDFrame{m_data.first, m_arithOp->round_binary(options)};
        }

        //--------------------------------------------------------------------------
        // 7) Trig
        //--------------------------------------------------------------------------
        NDFrame cos() const { return NDFrame{m_data.first, m_arithOp->cos()}; }

        NDFrame sin() const { return NDFrame{m_data.first, m_arithOp->sin()}; }

        NDFrame tan() const { return NDFrame{m_data.first, m_arithOp->tan()}; }

        NDFrame acos() const { return NDFrame{m_data.first, m_arithOp->acos()}; }

        NDFrame asin() const { return NDFrame{m_data.first, m_arithOp->asin()}; }

        NDFrame atan() const { return NDFrame{m_data.first, m_arithOp->atan()}; }

        NDFrame atan2(NDFrame const &other) const {
            return NDFrame{m_arithOp->atan2(other.m_data)};
        }

        // hyperbolic
        NDFrame sinh() const { return NDFrame{m_data.first, m_arithOp->sinh()}; }

        NDFrame cosh() const { return NDFrame{m_data.first, m_arithOp->cosh()}; }

        NDFrame tanh() const { return NDFrame{m_data.first, m_arithOp->tanh()}; }

        NDFrame acosh() const { return NDFrame{m_data.first, m_arithOp->acosh()}; }

        NDFrame asinh() const { return NDFrame{m_data.first, m_arithOp->asinh()}; }

        NDFrame atanh() const { return NDFrame{m_data.first, m_arithOp->atanh()}; }

        //--------------------------------------------------------------------------
        // 8) Cumulative
        //--------------------------------------------------------------------------
        NDFrame cumulative_sum(arrow::compute::CumulativeOptions const &options) const {
            return NDFrame{m_data.first, m_arithOp->cumulative_sum(options)};
        }

        NDFrame cumulative_prod(arrow::compute::CumulativeOptions const &options) const {
            return NDFrame{m_data.first, m_arithOp->cumulative_prod(options)};
        }

        NDFrame cumulative_max(arrow::compute::CumulativeOptions const &options) const {
            return NDFrame{m_data.first, m_arithOp->cumulative_max(options)};
        }

        NDFrame cumulative_min(arrow::compute::CumulativeOptions const &options) const {
            return NDFrame{m_data.first, m_arithOp->cumulative_min(options)};
        }

        NDFrame cumulative_mean(arrow::compute::CumulativeOptions const &options) const {
            return NDFrame{m_data.first, m_arithOp->cumulative_mean(options)};
        }

        //--------------------------------------------------------------------------
        // 9) Indexing ops
        //--------------------------------------------------------------------------
        Scalar iat(int64_t row, std::string const &col) const;

        //--------------------------------------------------------------------------
        // 10) Serialization
        //--------------------------------------------------------------------------
        friend std::ostream &operator<<(std::ostream &os, NDFrame const &);

        //--------------------------------------------------------------------------
        // 11) Comparison
        //--------------------------------------------------------------------------
        NDFrame operator==(NDFrame const &other) const { return NDFrame{m_compareOp->equal(other.m_data)}; }

        NDFrame operator==(Scalar const &other) const { return NDFrame{m_data.first, m_compareOp->equal(other)}; }

        friend NDFrame operator==(Scalar const &lhs, NDFrame const &rhs) {
            return NDFrame{rhs.m_data.first, rhs.m_compareOp->requal(lhs)};
        }

        NDFrame operator!=(NDFrame const &other) const { return NDFrame{m_compareOp->not_equal(other.m_data)}; }

        NDFrame operator!=(Scalar const &other) const { return NDFrame{m_data.first, m_compareOp->not_equal(other)}; }

        friend NDFrame operator!=(Scalar const &lhs, NDFrame const &rhs) {
            return NDFrame{rhs.m_data.first, rhs.m_compareOp->not_equal(lhs)};
        }

        NDFrame operator<(NDFrame const &other) const { return NDFrame{m_compareOp->less(other.m_data)}; }

        NDFrame operator<(Scalar const &other) const { return NDFrame{m_data.first, m_compareOp->less(other)}; }

        friend NDFrame operator<(Scalar const &lhs, NDFrame const &rhs) {
            return NDFrame{rhs.m_data.first, rhs.m_compareOp->rless(lhs)};
        }

        NDFrame operator<=(NDFrame const &other) const { return NDFrame{m_compareOp->less_equal(other.m_data)}; }

        NDFrame operator<=(Scalar const &other) const { return NDFrame{m_data.first, m_compareOp->less_equal(other)}; }

        friend NDFrame operator<=(Scalar const &lhs, NDFrame const &rhs) {
            return NDFrame{rhs.m_data.first, rhs.m_compareOp->rless_equal(lhs)};
        }

        NDFrame operator>(NDFrame const &other) const { return NDFrame{m_compareOp->greater(other.m_data)}; }

        NDFrame operator>(Scalar const &other) const { return NDFrame{m_data.first, m_compareOp->greater(other)}; }

        friend NDFrame operator>(Scalar const &lhs, NDFrame const &rhs) {
            return NDFrame{rhs.m_data.first, rhs.m_compareOp->rgreater(lhs)};
        }

        NDFrame operator>=(NDFrame const &other) const { return NDFrame{m_compareOp->greater_equal(other.m_data)}; }

        NDFrame operator>=(Scalar const &other) const {
            return NDFrame{m_data.first, m_compareOp->greater_equal(other)};
        }

        friend NDFrame operator>=(Scalar const &lhs, NDFrame const &rhs) {
            return NDFrame{rhs.m_data.first, rhs.m_compareOp->rgreater_equal(lhs)};
        }

        //========================
        // 12) Logical ops (and/or/xor)
        //========================
        NDFrame operator&&(NDFrame const &other) const { return NDFrame{m_compareOp->and_(other.m_data)}; }

        NDFrame operator&&(Scalar const &other) const { return NDFrame{m_data.first, m_compareOp->and_(other)}; }

        friend NDFrame operator&&(Scalar const &lhs, NDFrame const &rhs) {
            return NDFrame{rhs.m_data.first, rhs.m_compareOp->rand_(lhs)};
        }

        NDFrame operator||(NDFrame const &other) const { return NDFrame{m_compareOp->or_(other.m_data)}; }

        NDFrame operator||(Scalar const &other) const { return NDFrame{m_data.first, m_compareOp->or_(other)}; }

        friend NDFrame operator||(Scalar const &lhs, NDFrame const &rhs) {
            return NDFrame{rhs.m_data.first, rhs.m_compareOp->ror_(lhs)};
        }

        NDFrame operator^(NDFrame const &other) const { return NDFrame{m_compareOp->xor_(other.m_data)}; }

        NDFrame operator^(Scalar const &other) const { return NDFrame{m_data.first, m_compareOp->xor_(other)}; }

        friend NDFrame operator^(Scalar const &lhs, NDFrame const &rhs) {
            return NDFrame{rhs.m_data.first, rhs.m_compareOp->rxor_(lhs)};
        }

        NDFrame operator!() const { return NDFrame{m_data.first, m_compareOp->invert()}; }

        //========================
        // 13) Common Operations
        //========================
        NDFrame is_finite() const { return NDFrame{m_data.first, m_commonOp->is_finite()}; }

        NDFrame is_inf() const { return NDFrame{m_data.first, m_commonOp->is_inf()}; }

        NDFrame is_nan() const { return NDFrame{m_data.first, m_commonOp->is_nan()}; }

        NDFrame is_null(arrow::compute::NullOptions const &option) const {
            return NDFrame{m_data.first, m_commonOp->is_null(option)};
        }

        NDFrame is_valid() const { return NDFrame{m_data.first, m_commonOp->is_valid()}; }

        NDFrame true_unless_null() const { return NDFrame{m_data.first, m_commonOp->true_unless_null()}; }

        NDFrame cast(arrow::compute::CastOptions const &option=arrow::compute::CastOptions()) const {
            return NDFrame{m_data.first, m_commonOp->cast(option)};
        }

    private:
        TableComponent m_data;
        std::shared_ptr<Arithmetic> m_arithOp;
        std::shared_ptr<Comparison> m_compareOp;
        std::shared_ptr<CommonOperations> m_commonOp;
    };

} // namespace epochframe
