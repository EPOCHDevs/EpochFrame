//
// Created by adesola on 2/13/25.
//
#include <epochframe/scalar.h>
#include <epoch_lab_shared/macros.h>
#include <arrow/api.h>
#include <arrow/compute/exec.h>
#include "common/asserts.h"


namespace epochframe {
    Scalar::Scalar() : m_scalar(arrow::MakeNullScalar(arrow::null())) {}

    Scalar::Scalar(const arrow::ScalarPtr &other) : m_scalar(other) {
        AssertWithTraceFromStream(other != nullptr, "Scalar pointer cannot be null");
    }

    //--------------------------------------------------------------------------
    // 1) Compare ops
    //--------------------------------------------------------------------------
    bool Scalar::operator==(Scalar const &other) const {
        return AssertCastScalarResultIsOk<arrow::BooleanScalar>(
                arrow::compute::CallFunction("equal", {this->m_scalar, other.m_scalar})).value;
    }
}
