//
// Created by adesola on 1/20/25.
//

#pragma once

#include <memory>
#include <utility>  // for std::pair


namespace arrow{
    class ChunkedArray;
    class Array;
    class Scalar;

    using ArrayPtr = std::shared_ptr<Array>;
    using ScalarPtr = std::shared_ptr<Scalar>;
}

// Forward-declare Index and NDFrame for pointer aliases:
namespace epochframe {

    class Index;

    class NDFrame;

// Common pointer types used throughout
    using IndexPtr = std::shared_ptr<Index>;
    using NDFramePtr = std::shared_ptr<NDFrame>;

    using Shape2D = std::array<size_t, 2>;
    using SliceType = std::pair<uint64_t , uint64_t>;
    using IndexType = uint64_t;
} // namespace epochframe
