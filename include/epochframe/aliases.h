//
// Created by adesola on 1/20/25.
//

#pragma once

#include <memory>
#include <utility>  // for std::pair

// Forward-declare Index and NDFrame for pointer aliases:
namespace epochframe {

    class Index;

    class NDFrame;

// Common pointer types used throughout
    using IndexPtr = std::shared_ptr<Index>;
    using NDFramePtr = std::shared_ptr<NDFrame>;

    using Shape2D = std::array<size_t, 2>;
} // namespace epochframe
