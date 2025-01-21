//
// Created by adesola on 1/20/25.
//

#include "string_index.h"

namespace epochframe {
    StringIndex::StringIndex(std::shared_ptr<arrow::StringArray> array)
            : ArrowIndex<arrow::StringArray>(std::move(array)) {}
}
