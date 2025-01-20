//
// Created by adesola on 1/20/25.
//

#include "epochframe/ndframe.h"
#include "epoch_lab_shared/macros.h"

namespace epochframe {

    NDFrame::NDFrame(IndexPtr index) : m_index(std::move(index)) {
        AssertWithTraceFromFormat(m_index != nullptr, "Index cannot be null");
    }
} // namespace epochframe

