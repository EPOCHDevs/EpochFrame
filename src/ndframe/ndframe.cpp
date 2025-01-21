//
// Created by adesola on 1/20/25.
//

#include "epochframe/ndframe.h"
#include "epoch_lab_shared/macros.h"
#include "factory/index_factory.h"


namespace epochframe {

    NDFrame::NDFrame(IndexPtr index) : m_index(index ? std::move(index) : factory::index::from_range(0)) {}
} // namespace epochframe
