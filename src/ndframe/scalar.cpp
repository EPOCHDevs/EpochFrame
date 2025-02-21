//
// Created by adesola on 2/13/25.
//
#include "epochframe/scalar.h"
#include <epoch_lab_shared/macros.h>
#include <arrow/api.h>
#include <arrow/compute/exec.h>
#include <methods/compare.h>

#include "common/asserts.h"
#include "common/arrow_compute_utils.h"
#include "epochframe/dataframe.h"
#include "epochframe/series.h"


namespace epochframe {

    template<typename T>
    requires std::is_scalar_v<T>
    arrow::ScalarPtr MakeScalar(T const &value) {
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

    // --- Non-Template Methods ---

    arrow::ScalarPtr Scalar::value() const {
        return m_scalar;
    }

    bool Scalar::operator==(Scalar const &other) const {
        // Call Arrow’s "equal" function and extract the Boolean value.
        auto result = arrow::compute::CallFunction("equal", {this->m_scalar, other.m_scalar});
        return AssertCastScalarResultIsOk<arrow::BooleanScalar>(result).value;
    }

    bool Scalar::operator<(Scalar const &other) const {
        return arrow_utils::call_compute_scalar_as<arrow::BooleanScalar>(
                {this->m_scalar, other.m_scalar}, "less").value;
    }

    bool Scalar::operator<=(Scalar const &other) const {
        return arrow_utils::call_compute_scalar_as<arrow::BooleanScalar>(
                {this->m_scalar, other.m_scalar}, "less_equal").value;
    }

    bool Scalar::operator>(Scalar const &other) const {
        return arrow_utils::call_compute_scalar_as<arrow::BooleanScalar>(
                {this->m_scalar, other.m_scalar}, "greater").value;
    }

    bool Scalar::operator>=(Scalar const &other) const {
        return arrow_utils::call_compute_scalar_as<arrow::BooleanScalar>(
                {this->m_scalar, other.m_scalar}, "greater_equal").value;
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
    // 5) Basic Arithmetic Ops
    //--------------------------------------------------------------------------

    Series Scalar::operator+(Series const &other) const {
        return other.radd(*this);
    }

    DataFrame Scalar::operator+(DataFrame const &other) const {
        return other.radd(*this);
    }

    Series Scalar::operator-(Series const &other) const {
        return other.rsubtract(*this);
    }

    DataFrame Scalar::operator-(DataFrame const &other) const {
        return other.rsubtract(*this);
    }

    Series Scalar::operator*(Series const &other) const {
        return other.rmultiply(*this);
    }

    DataFrame Scalar::operator*(DataFrame const &other) const {
        return other.rmultiply(*this);
    }

    Series Scalar::operator/(Series const &other) const {
        return other.rdivide(*this);
    }

    DataFrame Scalar::operator/(DataFrame const &other) const {
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
    Series Scalar::operator==(Series const &other) const {
        return other.from_base(other.comparator().requal(m_scalar));
    }

    DataFrame Scalar::operator==(DataFrame const &other) const {
        return other.from_base(other.comparator().requal(m_scalar));
    }

    Series Scalar::operator!=(Series const &other) const {
        return other.from_base(other.comparator().not_equal(m_scalar));
    }

    DataFrame Scalar::operator!=(DataFrame const &other) const {
        return other.from_base(other.comparator().not_equal(m_scalar));
    }

    Series Scalar::operator<(Series const &other) const {
        return other.from_base(other.comparator().less(m_scalar));
    }

    DataFrame Scalar::operator<(DataFrame const &other) const {
        return other.from_base(other.comparator().less(m_scalar));
    }

    Series Scalar::operator<=(Series const &other) const {
        return other.from_base(other.comparator().less_equal(m_scalar));
    }

    DataFrame Scalar::operator<=(DataFrame const &other) const {
        return other.from_base(other.comparator().less_equal(m_scalar));
    }

    Series Scalar::operator>(Series const &other) const {
        return other.from_base(other.comparator().greater(m_scalar));
    }

    DataFrame Scalar::operator>(DataFrame const &other) const {
        return other.from_base(other.comparator().greater(m_scalar));
    }

    Series Scalar::operator>=(Series const &other) const {
        return other.from_base(other.comparator().greater_equal(m_scalar));
    }

    DataFrame Scalar::operator>=(DataFrame const &other) const {
        return other.from_base(other.comparator().greater_equal(m_scalar));
    }

    DataFrame Scalar::operator||(DataFrame const &other) const {
        return other.from_base(other.comparator().or_(m_scalar));
    }

    DataFrame Scalar::operator&&(DataFrame const &other) const {
        return other.from_base(other.comparator().and_(m_scalar));
    }

    DataFrame Scalar::operator^(DataFrame const &other) const {
        return other.from_base(other.comparator().xor_(m_scalar));
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
} // namespace epochframe
