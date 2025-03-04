//
// Created by adesola on 2/16/25.
//

#pragma once
#include "epochframe/scalar.h"


namespace epochframe {
    using MonotonicIndexer = std::map<Scalar, int64_t>;
    using NonMonotonicIndexer = std::unordered_map<Scalar, int64_t, ScalarHash>;
}
