#pragma once
#include <cmath>


namespace epochframe
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
} // namespace epochframe
