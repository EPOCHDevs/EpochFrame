#pragma once
#include <cmath>
#include <vector>
#include <type_traits>
#include "date_time/holiday/holiday_data.h"


namespace epoch_frame
{
    auto pymod(auto a, auto b)
    {
        return ((a % b) + b) % b;
    }

    auto round(auto num, auto digits)
    {
        auto multiplier = std::pow(10, digits);
        return std::round(num * multiplier) / multiplier;
    }

    auto floor_div(auto a, auto b)
    {
        auto result = static_cast<double>(a) / static_cast<double>(b);
        return std::floor(result);
    }


    // Generic chain implementation for Python-like itertools.chain functionality

    // Type trait to check if T is a container with begin() and end() methods
    template <typename T, typename = void>
    struct is_container : std::false_type {};

    template <typename T>
    struct is_container<T, std::void_t<
        decltype(std::declval<T>().begin()),
        decltype(std::declval<T>().end())
    >> : std::true_type {};

    // Type trait to check if T can be converted to a container of elements
    template <typename T, typename ElementType, typename = void>
    struct is_convertible_to_container : std::false_type {};

    // Add specialization for HolidayData if needed - this would define how a HolidayData
    // can be converted to a container of the target element type

    // Recursive case for multiple arguments
    template<typename First, typename ... Rest>
    First chain(First first, Rest &&... rest) {
        if constexpr (sizeof...(Rest) > 0) {
            auto result = chain(rest...);
            first.insert(first.end(), result.begin(), result.end());
        }
        return first;
    }
} // namespace epoch_frame
