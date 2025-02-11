//
// Created by adesola on 2/9/25.
//

#include "datetime_busday.h"


namespace epochframe::datetime {
    boost::gregorian::date busday_offset(boost::gregorian::date const& date,
                                         int64_t offset,
                                         bool rollForward,
                                         std::optional<BusDayCalendar> const& busdaycal) {
        return date;
    }

    bool is_busday(boost::gregorian::date const &date,
                   std::optional<BusDayCalendar> const &) {
        return true;
    }
}
