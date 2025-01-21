//
// Created by adesola on 1/20/25.
//

#pragma once
#include "arrow_index.h"


namespace epochframe {

    class ObjectIndex : public ArrowIndex<arrow::StringArray> {

    public:
        explicit ObjectIndex(std::shared_ptr<arrow::StringArray> array);

    };
} // namespace epochframe
