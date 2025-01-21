//
// Created by adesola on 1/20/25.
//

#pragma once
#include "index.h"
#include "arrow_index.h"


namespace epochframe {

    class RangeIndex : public ArrowIndex<arrow::UInt64Array> {
    public:
        explicit RangeIndex(std::shared_ptr<arrow::UInt64Array> array);

    };
} // namespace epochframe
