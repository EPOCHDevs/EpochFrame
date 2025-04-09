#pragma once
#include <cstdint>
#include "day_of_week.h"
#include <optional>
#include "datetime.h"

namespace epoch_frame {
    struct RelativeDeltaOption {
        std::optional<DateTime> dt1{std::nullopt};
        std::optional<DateTime> dt2{std::nullopt};
        double years{0};
        double months{0};
        double days{0};
        double leapdays{0};
        double weeks{0};
        double hours{0};
        double minutes{0};
        double seconds{0};
        double microseconds{0};
        std::optional<uint32_t> year{std::nullopt};
        std::optional<uint32_t> month{std::nullopt};
        std::optional<uint32_t> day{std::nullopt};
        std::optional<Weekday> weekday{std::nullopt};
        std::optional<int64_t> yearday{std::nullopt};
        std::optional<int64_t> nlyearday{std::nullopt};
        std::optional<int64_t> hour{std::nullopt};
        std::optional<int64_t> minute{std::nullopt};
        std::optional<int64_t> second{std::nullopt};
        std::optional<int64_t> microsecond{std::nullopt};
    };
}