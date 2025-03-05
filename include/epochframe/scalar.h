//
// Created by adesola on 2/13/25.
//
#pragma once

#include "aliases.h"
#include <numeric>
#include <optional>
#include <type_traits>
#include <string>
#include <arrow/scalar.h>
#include <methods/temporal.h>


namespace epochframe {

    template<typename T>
    requires std::is_scalar_v<T>
    arrow::ScalarPtr MakeScalar(T const &value);

    arrow::ScalarPtr MakeStructScalar(std::unordered_map<std::string, Scalar> const &other);

    class Scalar {
    public:
        // ------------------------------------------------------------------------
        // Default and non-template constructors
        // ------------------------------------------------------------------------
        Scalar();

        explicit Scalar(const arrow::ScalarPtr &other);

        explicit Scalar(std::string const &other);

        explicit Scalar(std::vector<std::pair<std::string, Scalar>> const &other);

        // Template constructor for primitive types.
        // (Definition is inline.)
        template<typename T>
        requires std::is_scalar_v<T>
        explicit Scalar(T &&other)
                : Scalar(MakeScalar(std::forward<T>(other))) {}

        // ------------------------------------------------------------------------
        // General Attributes
        // ------------------------------------------------------------------------
        [[nodiscard]] arrow::ScalarPtr value() const;

        template<typename T>
        requires (std::is_scalar_v<T> || std::is_same_v<T, std::string>)
        std::optional<T> value() const;

        bool is_valid() const;

        bool is_null() const;

        bool is_type(arrow::DataTypePtr const &type) const;

        arrow::DataTypePtr type() const;

        std::string repr() const;

        //--------------------------------------------------------------------------
        // 4) Basic Unary Ops
        //--------------------------------------------------------------------------
        Scalar abs() const;

        Scalar operator-() const;

        Scalar sign() const;

        //--------------------------------------------------------------------------
        // 5) Basic Arithmetic Ops
        //--------------------------------------------------------------------------
        Scalar operator+(Scalar const &other) const;

         Series operator+(Series const&) const;

        DataFrame operator+(DataFrame const&) const;

        Scalar operator-(Scalar const &other) const;

         Series operator-(Series const&) const;

         DataFrame operator-(DataFrame const&) const;

        Scalar operator*(Scalar const &other) const;

         Series operator*(Series const&) const;

         DataFrame operator*(DataFrame const&) const;

        Scalar operator/(Scalar const &other) const;

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

        bool operator!=(Scalar const &other) const;

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

        //--------------------------------------------------------------------------
        // 2) Serialization
        //--------------------------------------------------------------------------
        friend std::ostream &operator<<(std::ostream &os, Scalar const &x) {
            return os << x.repr();
        }

        [[nodiscard]] TemporalOperation<false> dt() const {
            return TemporalOperation<false>(m_scalar);
        }

    private:
        arrow::ScalarPtr m_scalar;

        Scalar(arrow::Result<arrow::Datum> const &scalar);
    };

    struct ScalarHash {
        size_t operator()(Scalar const &x) const {
            return x.value()->hash();
        }
    };
    template<typename T>
    using ScalarMapping = std::unordered_map<Scalar, T, ScalarHash>;

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

    extern template std::optional<std::string> Scalar::value<std::string>() const;

    Scalar operator""_scalar(unsigned long long value);

    Scalar operator""_scalar(long double value);

    Scalar operator""_uscalar(unsigned long long value);

    Scalar operator""_scalar(const char* value, std::size_t N);
} // namespace epochframe
