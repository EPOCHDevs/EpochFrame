//
// Created by adesola on 1/21/25.
//

#pragma once
#include <boost/date_time/posix_time/posix_time.hpp>
#include <boost/date_time/gregorian/gregorian.hpp>

using namespace boost::posix_time;
using namespace boost::gregorian;

namespace epochframe::datetime {
    enum class DateOfMonthOption {
        Start,
        End,
        BusinessStart,
        BusinessEnd,
    };

    int get_day_of_month(date const& , DateOfMonthOption) noexcept;
}
