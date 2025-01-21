//
// Created by adesola on 1/20/25.
//

#include "range_index.h"

namespace epochframe {
    RangeIndex::RangeIndex(std::shared_ptr<arrow::UInt64Array> array)
            : ArrowIndex<arrow::UInt64Array>(std::move(array)) {}
}
