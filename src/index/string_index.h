//
// Created by adesola on 1/20/25.
//

#pragma once
#include "arrow_index.h"


namespace epochframe {

    class StringIndex : public ArrowIndex<arrow::StringArray> {

    public:
        explicit StringIndex(std::shared_ptr<arrow::StringArray> array);

    };
} // namespace epochframe
