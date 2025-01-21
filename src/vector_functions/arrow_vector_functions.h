//
// Created by adesola on 1/20/25.
//

#pragma once
#include "epochframe/aliases.h"
#include "common_utils/asserts.h"
#include "arrow/compute/api.h"

namespace epochframe::vector {
    template<typename T>
    std::shared_ptr<T> unique(arrow::ArrayPtr const & array) {
        using namespace arrow::compute;
        return AssertCastArrayResultIsOk<T>(CallFunction("unique", {array}));
    }

}
