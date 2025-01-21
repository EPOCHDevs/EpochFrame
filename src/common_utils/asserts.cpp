//
// Created by adesola on 1/20/25.
//
#include "asserts.h"


namespace epochframe {
    void AssertStatusIsOk(const arrow::Status &status) {
        if (!status.ok()) throw std::runtime_error(status.ToString());
    }
}
