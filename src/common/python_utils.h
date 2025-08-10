#pragma once
#include "date_time/holiday/holiday_data.h"
#include <cmath>
#include <type_traits>
#include <utility>
#include <vector>

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

    // Python-style floor division returning both quotient and remainder
    // Ensures: a = q*b + r with q = floor(a/b) and r having the sign of b, 0 <= r < |b| if b>0
    template <typename T> std::pair<T, T> floor_div_rem(T a, T b)
    {
        static_assert(std::is_arithmetic_v<T>, "floor_div_rem requires arithmetic types");
        auto q =
            static_cast<T>(std::floor(static_cast<long double>(a) / static_cast<long double>(b)));
        auto r = static_cast<T>(a - q * b);
        return {q, r};
    }

    // Generic chain implementation for Python-like itertools.chain functionality

    // Type trait to check if T is a container with begin() and end() methods
    template <typename T, typename = void> struct is_container : std::false_type
    {
    };

    template <typename T>
    struct is_container<
        T, std::void_t<decltype(std::declval<T>().begin()), decltype(std::declval<T>().end())>>
        : std::true_type
    {
    };

    // Type trait to check if T can be converted to a container of elements
    template <typename T, typename ElementType, typename = void>
    struct is_convertible_to_container : std::false_type
    {
    };

    // Add specialization for HolidayData if needed - this would define how a HolidayData
    // can be converted to a container of the target element type

    // Recursive case for multiple arguments
    template <typename First, typename... Rest> First chain(First first, Rest&&... rest)
    {
        if constexpr (sizeof...(Rest) > 0)
        {
            auto result = chain(rest...);
            first.insert(first.end(), result.begin(), result.end());
        }
        return first;
    }
} // namespace epoch_frame
