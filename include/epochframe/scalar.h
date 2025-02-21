//
// Created by adesola on 2/13/25.
//
#pragma once

#include "aliases.h"
#include <numeric>
#include <optional>
#include <type_traits>
#include <string>
#include <iostream>
#include <arrow/compute/api.h>
#include "common/asserts.h"


namespace epochframe {

    template<typename T>
    requires std::is_scalar_v<T>
    arrow::ScalarPtr MakeScalar(T const &value);

    class Scalar {
    public:
        // Default and non-template constructors
        Scalar();

        explicit Scalar(const arrow::ScalarPtr &other);

        explicit Scalar(std::string const &other);

        // Template constructor for primitive types.
        // (Definition is inline.)
        template<typename T>
        requires std::is_scalar_v<T>
        explicit Scalar(T &&other)
                : Scalar(MakeScalar(std::forward<T>(other))) {}

        // Returns the underlying Arrow scalar pointer.
        [[nodiscard]] arrow::ScalarPtr value() const;


        //--------------------------------------------------------------------------
        // 2) Serialization
        //--------------------------------------------------------------------------
        friend std::ostream &operator<<(std::ostream &os, Scalar const &x) {
            return os << x.repr();
        }

        //--------------------------------------------------------------------------
        // 3) General Attributes
        //--------------------------------------------------------------------------
        // This templated method extracts the value as a C++ primitive type.
        template<typename T>
        requires std::is_scalar_v<T>
        std::optional<T> value() const;

        bool is_valid() const;

        bool is_null() const;

        bool is_type(arrow::DataTypePtr const &type) const;

        arrow::DataTypePtr type() const;

        std::string repr() const;

        //--------------------------------------------------------------------------
        // 4) Basic Unary Ops
        //--------------------------------------------------------------------------
        Scalar abs() const {
            return arrow::compute::AbsoluteValue(m_scalar);
        }

        Scalar operator-() const {
            return arrow::compute::Negate(m_scalar);
        }

        Scalar sign() const {
            return arrow::compute::Sign(m_scalar);
        }

        //--------------------------------------------------------------------------
        // 5) Basic Arithmetic Ops
        //--------------------------------------------------------------------------
        Scalar operator+(Scalar const &other) const {
            return arrow::compute::Add(m_scalar, other.m_scalar);
        }

         Series operator+(Series const&) const;

        DataFrame operator+(DataFrame const&) const;

        Scalar operator-(Scalar const &other) const {
            return arrow::compute::Subtract(m_scalar, other.m_scalar);
        }

         Series operator-(Series const&) const;

         DataFrame operator-(DataFrame const&) const;

        Scalar operator*(Scalar const &other) const {
            return arrow::compute::Multiply(m_scalar, other.m_scalar);
        }

         Series operator*(Series const&) const;

         DataFrame operator*(DataFrame const&) const;

        Scalar operator/(Scalar const &other) const {
            return arrow::compute::Divide(m_scalar, other.m_scalar);
        }

         Series operator/(Series const&) const;

         DataFrame operator/(DataFrame const&) const;

        //--------------------------------------------------------------------------
        // 3) Power
        //--------------------------------------------------------------------------

        Series power(Series const&) const;

        DataFrame power(DataFrame const&) const;

        Series logb(Series const&) const;

        DataFrame logb(DataFrame const&) const;

        //--------------------------------------------------------------------------
        // 10) Comparison ops
        //--------------------------------------------------------------------------
        bool operator==(Scalar const &other) const;

        Series operator==(Series const &other) const;

        DataFrame operator==(DataFrame const &other) const;

        bool operator!=(Scalar const &other) const;

        Series operator!=(Series const &other) const;

        DataFrame operator!=(DataFrame const &other) const;

        bool operator<(Scalar const &other) const;

        Series operator<(Series const &other) const;

        DataFrame operator<(DataFrame const &other) const;

        bool operator<=(Scalar const &other) const;

        Series operator<=(Series const &other) const;

        DataFrame operator<=(DataFrame const &other) const;

        bool operator>(Scalar const &other) const;

        Series operator>(Series const &other) const;

        DataFrame operator>(DataFrame const &other) const;

        bool operator>=(Scalar const &other) const;

        Series operator>=(Series const &other) const;

        DataFrame operator>=(DataFrame const &other) const;

        //--------------------------------------------------------------------------
        // 11) Logical ops (and/or/xor)
        //--------------------------------------------------------------------------

        Scalar operator&&(Scalar const &other) const;

        Series operator&&(Series const &other) const;

        DataFrame operator&&(DataFrame const &other) const;

        Scalar operator||(Scalar const &other) const;

        Series operator||(Series const &other) const;

        DataFrame operator||(DataFrame const &other) const;

        Scalar operator^(Scalar const &other) const;

        Series operator^(Series const &other) const;

        DataFrame operator^(DataFrame const &other) const;

        Scalar operator!() const;

    private:
        arrow::ScalarPtr m_scalar;

        Scalar(arrow::Result<arrow::Datum> const &scalar):m_scalar(AssertScalarResultIsOk(scalar)) {}
    };

    extern template arrow::ScalarPtr MakeScalar<>(uint64_t const &value);
    extern template std::optional<uint64_t> Scalar::value<uint64_t>() const;

    extern template arrow::ScalarPtr MakeScalar<>(uint32_t const &value);
    extern template std::optional<uint32_t> Scalar::value<uint32_t>() const;

    extern template arrow::ScalarPtr MakeScalar<>(int64_t const &value);
    extern template std::optional<int64_t> Scalar::value<int64_t>() const;

    extern template arrow::ScalarPtr MakeScalar<>(int32_t const &value);
    extern template std::optional<int32_t> Scalar::value<int32_t>() const;

    extern template arrow::ScalarPtr MakeScalar<>(double const &value);
    extern template std::optional<double> Scalar::value<double>() const;

    extern template arrow::ScalarPtr MakeScalar<>(float const &value);
    extern template std::optional<float> Scalar::value<float>() const;

    extern template arrow::ScalarPtr MakeScalar<>(bool const &value);
    extern template std::optional<bool> Scalar::value<bool>() const;

    inline Scalar operator""_scalar(unsigned long long value) {
        return Scalar(static_cast<int64_t>(value));
    }

    inline Scalar operator""_scalar(long double value) {
        return Scalar(static_cast<double>(value));
    }

    inline Scalar operator""_uscalar(unsigned long long value) {
        return Scalar(static_cast<uint64_t>(value));
    }

    inline Scalar operator""_scalar(const char* value, std::size_t N) {
        return Scalar(std::string(value, N ));
    }

} // namespace epochframe
