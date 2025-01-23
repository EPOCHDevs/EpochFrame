//
// Created by adesola on 1/21/25.
//

#include "ptime_common.h"
#include "calendar.h"


namespace epochframe::datetime {
    int get_day_of_month(date const &_date, DateOfMonthOption option) noexcept {
        switch (option) {
            case DateOfMonthOption::Start:
                return 1;
            case DateOfMonthOption::End:
                return get_days_in_month(_date.year(), _date.month());
            case DateOfMonthOption::BusinessStart:
                return get_firstbday(_date.year(), _date.month());
            case DateOfMonthOption::BusinessEnd:
                return get_lastbday(_date.year(), _date.month());
            default:
                break;
        }
        return 0;
    }
}
