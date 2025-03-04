//
// Created by adesola on 2/9/25.
//

#include "custom_business_day.h"
#include "multiarray/datetime_busday.h"


namespace epochframe::datetime {

    Timestamp CustomBusinessDayHandler::apply(const epochframe::datetime::Timestamp &value) const {
        bool rollForward = (n() <= 0);
        auto date_in = value.value();

        auto np_dt = date_in.date();
        auto dt_date = busday_offset(np_dt, n(), rollForward, m_calendar);

        Timestamp result{ptime(dt_date, date_in.time_of_day())};

        if (m_offset) {
            return result + (*m_offset);
        }
        return result;
    }

    bool CustomBusinessDayHandler::is_on_offset(const epochframe::datetime::Timestamp &value) const {
        if (should_normalize() && !is_normalized(value)) {
            return false;
        }
        return is_busday(value.value().date(), m_calendar);
    }
}
