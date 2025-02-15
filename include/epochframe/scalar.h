//
// Created by adesola on 2/13/25.
//

#pragma once
#include "aliases.h"


namespace epochframe {
    class Scalar {
    public:
        Scalar();

        explicit Scalar(const arrow::ScalarPtr &other);

        [[nodiscard]] arrow::ScalarPtr value() const {
            return m_scalar;
        }

    private:
        arrow::ScalarPtr m_scalar;
    };
}
