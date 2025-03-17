#pragma once

#include <string>
#include "../date_offsets.h"
#include <optional>
#include "../day_of_week.h"
#include <vector>
#include <set>
#include "epochframe/aliases.h"
#include "epochframe/scalar.h"


namespace epochframe
{
    using Observance = std::function<DateTime(DateTime const&)>;
    struct HolidayData {
       std::string name;
       std::optional<chrono_year> year;
       chrono_month month;
       chrono_day day;
       DateOffsetHandlerPtrs offset;
       std::optional<DateTime> start_date{std::nullopt};
       std::optional<DateTime> end_date{std::nullopt};
       Observance observance{nullptr};
       std::set<EpochDayOfWeek> days_of_week;
    };
} // namespace epochframe