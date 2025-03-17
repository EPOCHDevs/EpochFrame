//
// Created by adesola on 2/9/25.
//

#pragma once
#include <boost/date_time/gregorian/gregorian.hpp>
#include "calendar/bus_day_calendar.h"
#include <optional>


namespace epochframe::datetime {
    boost::gregorian::date busday_offset(boost::gregorian::date const &date,
                                         int64_t offset,
                                         bool rollForward,
                                         std::optional<BusDayCalendar> const &);

    bool is_busday(boost::gregorian::date const &date,
                   std::optional<BusDayCalendar> const &);
}
