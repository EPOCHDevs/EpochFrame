//
// Created by adesola on 1/20/25.
//

#include "datetime_index.h"


namespace epochframe {

    DateTimeIndex::DateTimeIndex(std::shared_ptr<arrow::TimestampArray> array) : ArrowIndex<arrow::TimestampArray>(
            std::move(array)) {}
} // namespace epochframe
