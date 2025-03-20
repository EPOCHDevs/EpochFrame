//
// Created by adesola on 2/17/25.
//
#include "epochframe/integer_slice.h"
#include <stdexcept>
#include <algorithm>


namespace epochframe {
    ResolvedIntegerSliceBound resolve_integer_slice(UnResolvedIntegerSliceBound const &bound, size_t length) {

        // Convert length to int64_t for arithmetic.
        int64_t len = static_cast<int64_t>(length);
        const auto [start, stop, step] = bound;

        // Determine the step; default to 1 if not provided.
        int64_t effective_step = step.value_or(1);
        if (effective_step == 0) {
            throw std::invalid_argument("slice step cannot be zero");
        }

        int64_t effective_start, effective_stop;

        if (effective_step > 0) {
            // For positive step:
            // Default start is 0, default stop is len.
            if (start.has_value()) {
                effective_start = start.value();
                if (effective_start < 0)
                    effective_start += len;
                // Clamp to [0, len]
                effective_start = std::clamp(effective_start, static_cast<int64_t>(0), len);
            } else {
                effective_start = 0;
            }
            if (stop.has_value()) {
                effective_stop = stop.value();
                if (effective_stop < 0)
                    effective_stop += len;
                // Clamp to [0, len]
                effective_stop = std::clamp(effective_stop, static_cast<int64_t>(0), len);
            } else {
                effective_stop = len;
            }
        } else {
            // For negative step:
            // Default start is len-1, default stop is -1.
            if (start.has_value()) {
                effective_start = start.value();
                if (effective_start < 0)
                    effective_start += len;
                // Clamp to [0, len-1]
                effective_start = std::clamp(effective_start, static_cast<int64_t>(0), len - 1);
            } else {
                effective_start = len - 1;
            }
            if (stop.has_value()) {
                effective_stop = stop.value();
                if (effective_stop < 0)
                    effective_stop += len;
                // For negative step, the stop index is clamped to [-1, len-1]
                effective_stop = std::min(effective_stop, len - 1);
                effective_stop = (effective_stop < -1 ? -1 : effective_stop);
            } else {
                effective_stop = -1;
            }
        }
        return ResolvedIntegerSliceBound{static_cast<uint64_t>(effective_start), static_cast<uint64_t>(effective_stop),
                                         static_cast<uint64_t>(effective_step)};
    }
}
