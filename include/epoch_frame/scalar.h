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
#include <epoch_frame/datetime.h>
#include <flatbuffers/array.h>
#include "epoch_frame/day_of_week.h"

namespace epoch_frame {

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

        explicit Scalar(const arrow::TimestampScalar &other);

        explicit Scalar(const arrow::DurationScalar &other);

        explicit Scalar(const Date &other);

        explicit Scalar(const DateTime &other);

        explicit Scalar(const TimeDelta &other);

        explicit Scalar(std::string const &other);

        explicit Scalar(std::vector<std::pair<std::string, Scalar>> const &other);

        // Template constructor for primitive types.
        // (Definition is inline.)
        template<typename T>
        requires std::is_scalar_v<T>
        explicit Scalar(T other)
                : Scalar(MakeScalar(std::move(other))) {}

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

        bool operator!=(Scalar const &other) const{
            return !(*this == other);
        }

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

        Scalar cast(arrow::DataTypePtr const &type) const;

        Scalar cast_int64() const {
            return cast(arrow::int64());
        }

        Scalar cast_int32() const {
            return cast(arrow::int32());
        }

        int64_t get_month_interval() const;

        Scalar cast_uint64() const {
            return cast(arrow::uint64());
        }

        Scalar cast_uint32() const {
            return cast(arrow::uint32());
        }

        Scalar cast_double() const {
            return cast(arrow::float64());
        }

        Scalar cast_float() const {
            return cast(arrow::float32());
        }

        [[nodiscard]] TemporalOperation<false> dt() const;

        [[nodiscard]] StringOperation<false> str() const;

        [[nodiscard]] arrow::TimestampScalar timestamp(std::string const &format="%Y-%m-%d %H:%M:%S",
            const std::string& tz="") const;

        DateTime to_datetime(std::string const &format="%Y-%m-%d %H:%M:%S", const std::string& tz="") const;
        DateTime to_date(std::string const &format="%Y-%m-%d", const std::string& tz="") const;

        [[nodiscard]] epoch_core::EpochDayOfWeek weekday() const;

        class Array to_array(int64_t lenght) const;

        double as_double() const {
            return value<double>().value_or(std::numeric_limits<double>::quiet_NaN());
        }

        float as_float() const {
            return get_or_throw_value<float>();
        }

        int64_t as_int64() const {
            return get_or_throw_value<int64_t>();
        }

        int32_t as_int32() const {
            return get_or_throw_value<int32_t>();
        }

        bool as_bool() const {
            return get_or_throw_value<bool>();
        }

    private:
        arrow::ScalarPtr m_scalar;

        Scalar(arrow::Result<arrow::Datum> const &scalar);

        template<typename T>
        T get_or_throw_value() const {
            try {
                return value<T>().value();
            } catch (std::exception const &e) {
                throw std::runtime_error(std::format("Failed to convert scalar to {}: {}", typeid(T).name(), e.what()));
            }
        }
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

    class TimeDelta operator-(arrow::TimestampScalar const &a, arrow::TimestampScalar const &b);
    arrow::TimestampScalar operator+(arrow::TimestampScalar const &a, TimeDelta const &b);
    arrow::TimestampScalar operator-(arrow::TimestampScalar const &a, TimeDelta const &b);
} // namespace epoch_frame
