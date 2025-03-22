#pragma once

#include <string>
#include "date_time/date_offsets.h"
#include <optional>
#include "date_time/day_of_week.h"
#include <vector>
#include <set>
#include "epoch_frame/aliases.h"

namespace epoch_frame
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
} // namespace epoch_frame
