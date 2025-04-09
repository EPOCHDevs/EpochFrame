#pragma once

#include "../date_offsets.h"
#include "epoch_frame/day_of_week.h"
#include "epoch_frame/aliases.h"
#include <optional>
#include <set>
#include <string>
#include <vector>

namespace epoch_frame {
    using Observance = std::function<std::optional<DateTime>(DateTime const&)>;
    struct HolidayData
    {
        std::string                name;
        std::optional<chrono_year> year{std::nullopt};
        std::optional<chrono_month>               month;
        std::optional<chrono_day>                 day;
        DateOffsetHandlerPtrs      offset{};
        std::optional<DateTime>    start_date{std::nullopt};
        std::optional<DateTime>    end_date{std::nullopt};
        Observance                 observance{nullptr};
        np::WeekSet                days_of_week{};
    };
} // namespace epoch_frame
