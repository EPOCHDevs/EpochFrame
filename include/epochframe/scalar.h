//
// Created by adesola on 2/13/25.
//

#pragma once

#include <arrow/scalar.h>
#include "aliases.h"
#include <numeric>

namespace epochframe {
    class Scalar {
    public:
        Scalar();

        explicit Scalar(const arrow::ScalarPtr &other);

        template<typename T>
        requires std::is_scalar_v<T>
        explicit Scalar(T &&other): Scalar(arrow::MakeScalar(std::forward<T>(other))) {}

        [[nodiscard]] arrow::ScalarPtr value() const {
            return m_scalar;
        }

        //--------------------------------------------------------------------------
        // 1) Compare ops
        //--------------------------------------------------------------------------
        bool operator==(Scalar const &other) const;

        //--------------------------------------------------------------------------
        // 2) Serialization
        //--------------------------------------------------------------------------
        friend std::ostream &operator<<(std::ostream &os, Scalar const &x) {
            return os << x.m_scalar->ToString();
        }

        //--------------------------------------------------------------------------
        // 3) General Attributes
        //--------------------------------------------------------------------------
        template<typename T>
        requires std::is_scalar_v<T>
        std::optional<T> value() const {
            auto scalar = std::dynamic_pointer_cast<typename arrow::CTypeTraits<T>::ScalarType>(m_scalar);
            return scalar ? std::make_optional(scalar->value) : std::nullopt;
        }

    private:
        arrow::ScalarPtr m_scalar;
    };

//    const Scalar INF{std::numeric_limits<double>::infinity()};
//    const Scalar NAN{std::numeric_limits<double>::quiet_NaN()};
}
