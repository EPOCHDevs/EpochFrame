//
// Created by adesola on 2/13/25.
//
#include <epochframe/scalar.h>
#include <epoch_lab_shared/macros.h>
#include <arrow/api.h>


namespace epochframe {
    Scalar::Scalar() : m_scalar(arrow::MakeNullScalar(arrow::null())) {}

    Scalar::Scalar(const arrow::ScalarPtr &other) {
        AssertWithTraceFromStream(other != nullptr, "Scalar pointer cannot be null");
    }
}
