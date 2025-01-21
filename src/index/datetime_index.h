//
// Created by adesola on 1/20/25.
//

#pragma once
#include "arrow_index.h"


namespace epochframe {

    class DateTimeIndex : public ArrowIndex<arrow::TimestampArray> {

    public:
        explicit DateTimeIndex(std::shared_ptr<arrow::TimestampArray> array);

    };
} // namespace epochframe
