//
// Created by adesola on 2/17/25.
//
#include "epoch_frame/integer_slice.h"
#include <stdexcept>
#include <algorithm>
#include <iostream>


namespace epoch_frame {
    ResolvedIntegerSliceBound resolve_integer_slice(UnResolvedIntegerSliceBound const &bound, size_t length) {

        // Convert length to int64_t for arithmetic.
        auto len = static_cast<int64_t>(length);
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
                effective_start = std::max(static_cast<int64_t>(0), effective_start);
            } else {
                effective_start = 0;
            }
            if (stop.has_value()) {
                effective_stop = stop.value();
                if (effective_stop < 0)
                    effective_stop += len;
                // Clamp stop to [0, len]
                effective_stop = std::min(len, std::max(static_cast<int64_t>(0), effective_stop));
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
                effective_start = std::min(len - 1, std::max(static_cast<int64_t>(0), effective_start));
            } else {
                effective_start = len - 1;
            }
            if (stop.has_value()) {
                effective_stop = stop.value();
                if (effective_stop < 0)
                    effective_stop += len;
                // Just use the computed value for now
                effective_stop = std::min(len - 1, std::max(static_cast<int64_t>(-1), effective_stop));
            } else {
                effective_stop = -1;
            }
        }

        // Print debug info for specific test case
        if (effective_step == -1 && start.has_value() && stop.has_value() && start.value() == 2 && stop.value() == -2) {
            std::cout << "Debug [2:-2:-1]: effective_start=" << effective_start
                      << ", effective_stop=" << effective_stop << std::endl;
        }

        // Calculate the length of the slice
        uint64_t slice_length = 0;
        if (effective_step > 0) {
            // Empty slice if start >= stop
            if (effective_start < effective_stop) {
                slice_length = (effective_stop - effective_start - 1) / effective_step + 1;
            }
        } else {
            // For negative step to work, we need start > stop
            if (effective_start > effective_stop) {
                if (effective_stop == -1) {
                    // For negative step with stop = -1, we go from start down to 0
                    slice_length = (effective_start + 1) / (-effective_step);
                    if ((effective_start + 1) % (-effective_step) != 0) {
                        slice_length += 1;
                    }
                } else {
                    slice_length = (effective_start - effective_stop - 1) / (-effective_step) + 1;
                }
            }
        }

        // Return the start, length and step
        return ResolvedIntegerSliceBound{
            static_cast<uint64_t>(effective_start),
            slice_length,
            effective_step
        };
    }
}

