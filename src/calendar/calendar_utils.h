//
// Created by adesola on 3/22/25.
//

#pragma once
#include "epoch_frame/index.h"
#include "date_time/holiday/holiday_calendar.h"


namespace epoch_frame::calendar::utils {

    std::optional<DateTime> is_single_observance(const HolidayData& cal);
    std::optional<std::vector<DateTime>> all_single_observance_rules(const AbstractHolidayCalendar& cal);

    struct DateRangeHTFOptions {
        std::shared_ptr<CustomBusinessDay> calendar;
        Date start;
        std::optional<Date> end;
        std::optional<int64_t> periods;
    };

    IndexPtr date_range_htf(const DateRangeHTFOptions& options);

} // namespace epoch_frame::calendar::cal_utils
