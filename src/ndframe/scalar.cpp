//
// Created by adesola on 2/13/25.
//
#include "epochframe/scalar.h"
#include <epoch_lab_shared/macros.h>
#include <arrow/api.h>
#include <arrow/compute/exec.h>
#include <methods/compare.h>
#include <arrow/compute/api.h>
#include "common/asserts.h"
#include "common/arrow_compute_utils.h"
#include "epochframe/dataframe.h"
#include "epochframe/series.h"


namespace epochframe {

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

    // --- Constructors ---

    Scalar::Scalar()
            : m_scalar(arrow::MakeNullScalar(arrow::null())) {}

    Scalar::Scalar(const arrow::ScalarPtr &other)
            : m_scalar(other) {
        AssertWithTraceFromStream(other != nullptr, "Scalar pointer cannot be null");
    }

    Scalar::Scalar(std::string const &other)
            : Scalar(arrow::MakeScalar(other)) {}

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

        // Call Arrow’s "equal" function and extract the Boolean value.
        auto result = arrow::compute::CallFunction("equal", {this->m_scalar, other.m_scalar});
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
    requires std::is_scalar_v<T>
    std::optional<T> Scalar::value() const {
        // Attempt to cast the internal Arrow scalar to the appropriate type.
        auto casted = std::dynamic_pointer_cast<typename arrow::CTypeTraits<T>::ScalarType>(m_scalar);
        return casted ? std::make_optional(casted->value) : std::nullopt;
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
} // namespace epochframe
