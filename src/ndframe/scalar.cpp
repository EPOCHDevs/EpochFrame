//
// Created by adesola on 2/13/25.
//
#include "epoch_frame/scalar.h"
#include <epoch_core/macros.h>
#include <arrow/api.h>
#include <arrow/compute/exec.h>
#include <methods/compare.h>
#include <methods/temporal.h>
#include <arrow/compute/api.h>
#include "common/asserts.h"
#include "common/arrow_compute_utils.h"
#include "epoch_frame/dataframe.h"
#include "epoch_frame/series.h"
#include "date_time/time_delta.h"
#include "factory/scalar_factory.h"
#include "date_time/datetime.h"

namespace epoch_frame {

    template<typename T>
    requires std::is_scalar_v<T>
    arrow::ScalarPtr MakeScalar(T const &value) {
        if constexpr (std::numeric_limits<T>::has_quiet_NaN) {
            if (std::isnan(value)) {
                return MakeNullScalar(arrow::float64());
            }
        }

        return arrow::MakeScalar(value);
    }

    arrow::ScalarPtr MakeStructScalar(std::vector<std::pair<std::string, Scalar>> const &other) {
        std::vector<arrow::ScalarPtr> fields;
        StringVector field_names;
        arrow::StructScalar::ValueType values;
        for (auto const &[key, value] : other) {
            field_names.push_back(key);
            values.push_back(value.value());
        }
        auto result = arrow::StructScalar::Make(values, field_names);
        if (result.ok()) {
            return result.ValueOrDie();
        }
        throw std::runtime_error("Failed to make struct scalar" + result.status().message());
    }

    // --- Constructors ---

    Scalar::Scalar()
            : m_scalar(arrow::MakeNullScalar(arrow::null())) {}

    Scalar::Scalar(const arrow::ScalarPtr &other)
            : m_scalar(other) {
        AssertFromStream(other != nullptr, "Scalar pointer cannot be null");
    }

    Scalar::Scalar(const DateTime &other)
            : m_scalar(std::make_shared<arrow::TimestampScalar>(other.timestamp())) {}

    Scalar::Scalar(const Date& other):Scalar(DateTime(other)){}

    Scalar::Scalar(const arrow::TimestampScalar &other)
            : m_scalar(std::make_shared<arrow::TimestampScalar>(other)) {}

    Scalar::Scalar(const arrow::DurationScalar &other)
            : m_scalar(std::make_shared<arrow::DurationScalar>(other)) {}

    Scalar::Scalar(const TimeDelta &other)
            : m_scalar(std::make_shared<arrow::DurationScalar>(other.to_nanoseconds(), arrow::TimeUnit::NANO)) {}

    Scalar::Scalar(std::string const &other)
            : Scalar(arrow::MakeScalar(other)) {}

    Scalar::Scalar(std::vector<std::pair<std::string, Scalar>> const &other)
            : Scalar(MakeStructScalar(other)) {}

    Scalar::Scalar(arrow::Result<arrow::Datum> const &scalar):m_scalar(AssertScalarResultIsOk(scalar)) {}

    // --- Non-Template Methods ---

    arrow::ScalarPtr Scalar::value() const {
        return m_scalar;
    }

    bool Scalar::is_valid() const {
        return m_scalar->is_valid;
    }

    bool Scalar::is_null() const {
        return !m_scalar->is_valid;
    }

    bool Scalar::is_type(arrow::DataTypePtr const &type) const {
        return m_scalar->type->Equals(type);
    }

    int64_t Scalar::get_month_interval() const {
        auto month_interval = PtrCast<arrow::MonthIntervalScalar>(m_scalar);
        return month_interval->value;
    }

    Scalar Scalar::cast(arrow::DataTypePtr const &type) const {
        auto type_id = type->id();
        if (type->Equals(m_scalar->type) || is_list_like(type_id) || arrow::is_string(type_id)) {
            return *this;
        }
        return Scalar{AssertResultIsOk(m_scalar->CastTo(type))};
    }

    arrow::DataTypePtr Scalar::type() const {
        return m_scalar->type;
    }

    std::string Scalar::repr() const {
        return m_scalar->ToString();
    }


    //--------------------------------------------------------------------------
    // 4) Basic Unary Ops
    //--------------------------------------------------------------------------
    Scalar Scalar::abs() const {
        return arrow::compute::AbsoluteValue(m_scalar);
    }

    Scalar Scalar::operator-() const {
            return arrow::compute::Negate(m_scalar);
        }

    Scalar Scalar::sign() const {
        return arrow::compute::Sign(m_scalar);
    }

            //--------------------------------------------------------------------------
        // 5) Basic Arithmetic Ops
        //--------------------------------------------------------------------------
    Scalar Scalar::operator+(Scalar const &other) const {
        return arrow::compute::Add(m_scalar, other.m_scalar);
    }

    Series Scalar::operator+(Series const& other) const {
        return other.radd(*this);
    }

    DataFrame Scalar::operator+(DataFrame const& other) const {
        return other.radd(*this);
    }

    Scalar Scalar::operator-(Scalar const &other) const {
        return arrow::compute::Subtract(m_scalar, other.m_scalar);
    }

    Series Scalar::operator-(Series const& other) const {
        return other.rsubtract(*this);
    }

    DataFrame Scalar::operator-(DataFrame const& other) const {
        return other.rsubtract(*this);
    }

    Scalar Scalar::operator*(Scalar const &other) const {
        return arrow::compute::Multiply(m_scalar, other.m_scalar);
    }

    Series Scalar::operator*(Series const& other) const {
        return other.rmultiply(*this);
    }

    DataFrame Scalar::operator*(DataFrame const& other) const {
        return other.rmultiply(*this);
    }

    Scalar Scalar::operator/(Scalar const &other) const {
        return arrow::compute::Divide(m_scalar, other.m_scalar);
    }

    Series Scalar::operator/(Series const& other) const {
        return other.rdivide(*this);
    }

    DataFrame Scalar::operator/(DataFrame const& other) const {
        return other.rdivide(*this);
    }

    //--------------------------------------------------------------------------
    // 3) Power
    //--------------------------------------------------------------------------

    Series Scalar::power(Series const &other) const {
        return other.rpower(*this);
    }

    DataFrame Scalar::power(DataFrame const &other) const {
        return other.rpower(*this);
    }

    Series Scalar::logb(Series const &other) const {
        // return other.rlogb(*this);
        return Series();
    }

    DataFrame Scalar::logb(DataFrame const &other) const {
        // return other.rlogb(*this);
        return DataFrame();
    }

    //--------------------------------------------------------------------------
    // 10) Comparison ops
    //--------------------------------------------------------------------------
    bool Scalar::operator==(Scalar const &other) const {
        if (other.is_null() && is_null()) {
            return true;
        }

        // Call Arrow's "equal" function and extract the Boolean value.
        const auto result = arrow::compute::CallFunction("equal", {this->m_scalar, other.m_scalar});
        if (result.status().IsNotImplemented()) {
            return m_scalar->Equals(*other.m_scalar);
        }
        return AssertCastScalarResultIsOk<arrow::BooleanScalar>(result).value;
    }

    bool Scalar::operator<(Scalar const &other) const {
        return arrow_utils::call_compute_scalar_as<arrow::BooleanScalar>(
                {this->m_scalar, other.m_scalar}, "less").value;
    }

    Series Scalar::operator<(Series const &other) const {
        return other.rless(*this);
    }

    bool Scalar::operator<=(Scalar const &other) const {
        return arrow_utils::call_compute_scalar_as<arrow::BooleanScalar>(
                {this->m_scalar, other.m_scalar}, "less_equal").value;
    }

    DataFrame Scalar::operator<(DataFrame const &other) const {
        return other.rless(*this);
    }

    Series Scalar::operator<=(Series const &other) const {
        return other.rless_equal(*this);
    }

    DataFrame Scalar::operator<=(DataFrame const &other) const {
        return other.rless_equal(*this);
    }

    bool Scalar::operator>(Scalar const &other) const {
        return arrow_utils::call_compute_scalar_as<arrow::BooleanScalar>(
                {this->m_scalar, other.m_scalar}, "greater").value;
    }

    Series Scalar::operator>(Series const &other) const {
        return other.rgreater(*this);
    }

    DataFrame Scalar::operator>(DataFrame const &other) const {
        return other.rgreater(*this);
    }

    bool Scalar::operator>=(Scalar const &other) const {
        return arrow_utils::call_compute_scalar_as<arrow::BooleanScalar>(
                {this->m_scalar, other.m_scalar}, "greater_equal").value;
    }

    Series Scalar::operator>=(Series const &other) const {
        return other.rgreater_equal(*this);
    }

    DataFrame Scalar::operator>=(DataFrame const &other) const {
        return other.rgreater_equal(*this);
    }

    //--------------------------------------------------------------------------
    // 11) Logical ops (and/or/xor)
    //--------------------------------------------------------------------------
    Scalar Scalar::operator&&(Scalar const &other) const {
        return arrow::compute::And(m_scalar, other.m_scalar);
    }

    Series Scalar::operator&&(Series const &other) const {
        return other.rand(*this);
    }

    DataFrame Scalar::operator&&(DataFrame const &other) const {
        return other.rand(*this);
    }

    Scalar Scalar::operator||(Scalar const &other) const {
        return arrow::compute::Or(m_scalar, other.m_scalar);
    }

    Series Scalar::operator||(Series const &other) const {
        return other.ror(*this);
    }

    DataFrame Scalar::operator||(DataFrame const &other) const {
        return other.ror(*this);
    }

    Scalar Scalar::operator^(Scalar const &other) const {
        return arrow::compute::Xor(m_scalar, other.m_scalar);
    }

    Series Scalar::operator^(Series const &other) const {
        return other.rxor(*this);
    }

    DataFrame Scalar::operator^(DataFrame const &other) const {
        return other.rxor(*this);
    }

    Scalar Scalar::operator!() const {
        return arrow::compute::Invert(m_scalar);
    }

    // --- Template Method Definition ---

    template<typename T>
    requires (std::is_scalar_v<T> || std::is_same_v<T, std::string>)
    std::optional<T> Scalar::value() const {
        if constexpr (std::is_same_v<T, std::string>) {
            return std::make_optional(m_scalar->ToString());
        } else {
            // Attempt to cast the internal Arrow scalar to the appropriate type.
            auto casted = std::dynamic_pointer_cast<typename arrow::CTypeTraits<T>::ScalarType>(m_scalar);
            return casted ? std::make_optional(casted->value) : std::nullopt;
        }
    }

    TemporalOperation<false> Scalar::dt() const {
        if (!value() || !value()->type || value()->type->id() != arrow::Type::TIMESTAMP) {
            throw std::runtime_error("dt accessor can only be used with timestamp data");
        }

        return TemporalOperation<false>(*this);
    }

    StringOperation<false> Scalar::str() const {
        return StringOperation<false>(*this);
    }

    arrow::TimestampScalar Scalar::timestamp(std::string const &format) const {
        if (arrow::is_string(m_scalar->type->id())) {
            auto result = AssertCastScalarResultIsOk<arrow::TimestampScalar>(arrow::compute::Strptime(m_scalar, arrow::compute::StrptimeOptions{format, arrow::TimeUnit::NANO}));
            return result;
        }
        auto scalarPtr = std::dynamic_pointer_cast<arrow::TimestampScalar>(m_scalar);
        if (!scalarPtr) {
            throw std::runtime_error("Scalar is not a timestamp");
        }
        return *scalarPtr;
    }

    DateTime Scalar::to_datetime(std::string const &format) const {
        return factory::scalar::to_datetime(timestamp(format));
    }

    DateTime Scalar::to_date(std::string const &format) const {
        return factory::scalar::to_datetime(timestamp(format));
    }

    epoch_core::EpochDayOfWeek Scalar::weekday() const {
        return static_cast<epoch_core::EpochDayOfWeek>(to_datetime().weekday());
    }

    Array Scalar::to_array(int64_t length) const {
        auto arr = AssertContiguousArrayResultIsOk(arrow::MakeArrayFromScalar(*m_scalar, length));
        return Array(arr);
    }

    // --- Explicit Template Instantiations ---
    template arrow::ScalarPtr MakeScalar<>(uint64_t const &value);

    template std::optional<uint64_t> Scalar::value<uint64_t>() const;

    template arrow::ScalarPtr MakeScalar<>(uint32_t const &value);

    template std::optional<uint32_t> Scalar::value<uint32_t>() const;

    template arrow::ScalarPtr MakeScalar<>(int64_t const &value);

    template std::optional<int64_t> Scalar::value<int64_t>() const;

    template arrow::ScalarPtr MakeScalar<>(int32_t const &value);

    template std::optional<int32_t> Scalar::value<int32_t>() const;

    template arrow::ScalarPtr MakeScalar<>(double const &value);

    template std::optional<double> Scalar::value<double>() const;

    template arrow::ScalarPtr MakeScalar<>(float const &value);

    template std::optional<float> Scalar::value<float>() const;

    template arrow::ScalarPtr MakeScalar<>(bool const &value);

    template std::optional<bool> Scalar::value<bool>() const;

    template std::optional<std::string> Scalar::value<std::string>() const;

    Scalar operator""_scalar(unsigned long long value) {
        return Scalar(static_cast<int64_t>(value));
    }

    Scalar operator""_scalar(long double value) {
        return Scalar(static_cast<double>(value));
    }

    Scalar operator""_uscalar(unsigned long long value) {
        return Scalar(static_cast<uint64_t>(value));
    }

    Scalar operator""_scalar(const char* value, std::size_t N) {
        return Scalar(std::string(value, N ));
    }

     TimeDelta operator-(arrow::TimestampScalar const &a, arrow::TimestampScalar const &b) {
        auto dt0 = factory::scalar::to_datetime(a);
        auto dt1 = factory::scalar::to_datetime(b);

        auto ms = AssertCastScalarResultIsOk<arrow::Int64Scalar>(arrow::compute::MicrosecondsBetween(a, b)).value;
        ms = a.value < b.value ? -ms : ms;

        return TimeDelta{
            TimeDelta::Components{
                .microseconds = static_cast<double>(ms)
            }
        };
    }

    arrow::TimestampScalar operator+(arrow::TimestampScalar const &a, TimeDelta const &b) {
        auto ns = a.value + std::chrono::duration_cast<chrono_nanoseconds>(chrono_microseconds(b.to_microseconds())).count();
        return {ns, a.type};
    }

    arrow::TimestampScalar operator-(arrow::TimestampScalar const &a, TimeDelta const &b) {
        auto ns = a.value - std::chrono::duration_cast<chrono_nanoseconds>(chrono_microseconds(b.to_microseconds())).count();
        return {ns, a.type};
    }
} // namespace epoch_frame
