//
// Created by adesola on 1/20/25.
//

#include "object_index.h"

namespace epochframe {
    ObjectIndex::ObjectIndex(std::shared_ptr<arrow::StringArray> array)
            : ArrowIndex<arrow::StringArray>(std::move(array)) {}
}
