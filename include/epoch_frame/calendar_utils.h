//
// Created by adesola on 3/22/25.
//

#pragma once
#include "date_time/holiday/holiday_calendar.h"
#include "epoch_frame/dataframe.h"

namespace epoch_frame::calendar::utils
{

    std::optional<DateTime> is_single_observance(const HolidayData& cal);
    std::optional<std::vector<DateTime>>
    all_single_observance_rules(const AbstractHolidayCalendar& cal);

    struct DateRangeHTFOptions
    {
        std::shared_ptr<CustomBusinessDay> calendar;
        Date                               start;
        std::optional<Date>                end;
        std::optional<int64_t>             periods;
    };

    IndexPtr date_range_htf(const DateRangeHTFOptions& options);

    DataFrame merge_schedules(const std::vector<DataFrame>& schedules, bool outer = true);

} // namespace epoch_frame::calendar::utils
