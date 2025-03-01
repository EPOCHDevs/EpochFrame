//
// Created by adesola on 2/17/25.
//

#pragma once
#include <optional>
#include <cinttypes>


namespace epochframe {
    struct ResolvedIntegerSliceBound {
        uint64_t start;
        uint64_t stop;
        uint64_t step;
    };

    struct UnResolvedIntegerSliceBound {
        std::optional<int64_t> start;
        std::optional<int64_t> stop;
        std::optional<int64_t> step;
    };

    ResolvedIntegerSliceBound resolve_integer_slice(UnResolvedIntegerSliceBound const&, size_t length);

    inline uint64_t resolve_integer_index(int64_t index, size_t length){
        return index < 0 ? length + index : index;
    }
}
