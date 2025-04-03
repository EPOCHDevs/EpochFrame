//
// Created by adesola on 2/17/25.
//

#pragma once
#include <optional>
#include <cinttypes>


namespace epoch_frame {
    struct ResolvedIntegerSliceBound {
        uint64_t start;
        uint64_t length; // Number of elements in the slice
        int64_t step;
    };

    struct UnResolvedIntegerSliceBound {
        std::optional<int64_t> start{std::nullopt};
        std::optional<int64_t> stop{std::nullopt};
        std::optional<int64_t> step{std::nullopt};
    };

    ResolvedIntegerSliceBound resolve_integer_slice(UnResolvedIntegerSliceBound const&, size_t length);

    inline uint64_t resolve_integer_index(int64_t index, size_t length){
        return index < 0 ? length + index : index;
    }
}
