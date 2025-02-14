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

    private:
        arrow::ScalarPtr m_scalar;
    };
}
