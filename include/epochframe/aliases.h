//
// Created by adesola on 1/20/25.
//

#pragma once

#include <memory>
#include <utility>  // for std::pair
#include <vector>

namespace arrow{
    class ChunkedArray;
    class Array;
    class Scalar;
    class Table;
    class Schema;

    using ArrayPtr = std::shared_ptr<Array>;
    using ChunkedArrayPtr = std::shared_ptr<ChunkedArray>;
    using ScalarPtr = std::shared_ptr<Scalar>;
    using TablePtr = std::shared_ptr<Table>;
    using SchemaPtr = std::shared_ptr<Schema>;
}

// Forward-declare Index and NDFrame for pointer aliases:
namespace epochframe {

    class Index;

    class Scalar;

    class NDFrame;

// Common pointer types used throughout
    using IndexPtr = std::shared_ptr<Index>;
    using NDFramePtr = std::shared_ptr<NDFrame>;

    using IndexType = uint64_t;
    using Shape2D = std::array<size_t, 2>;
    using SliceType = std::pair<Scalar, Scalar>;
    using IntgerSliceType = std::pair<IndexType, IndexType>;

    using TableComponent = std::pair<IndexPtr, arrow::TablePtr>;
    using TableComponents = std::vector<TableComponent>;
} // namespace epochframe
