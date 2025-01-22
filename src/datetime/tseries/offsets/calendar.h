//
// Created by adesola on 1/21/25.
//

#pragma once
#include "stdint.h"
#include <array>

namespace epochframe::datetime {
    // ----------------------------------------------------------------------
// Static Data

// days_per_month_array has 24 entries:
//   First 12 for non-leap years, next 12 for leap years.
//   Index = 12 * isLeap + (month-1).
    static const int32_t DAYS_PER_MONTH_ARRAY[24] = {
            // non-leap (months 1..12)
            31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31,
            // leap year (months 1..12)
            31, 29, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31
    };

// month_offset has 26 entries:
//   First 13 for non-leap year, next 13 for leap year.
//   Each block is [month=0..12], where month_offset[idx] is the cumulative
//   days up to that month. The final entry is total days in that year (365/366).
    static const int32_t MONTH_OFFSET[26] = {
            // Non-leap: months 0..12
            0, 31, 59, 90, 120, 151, 181, 212, 243, 273, 304, 334, 365,
            // Leap year: months 0..12
            0, 31, 60, 91, 121, 152, 182, 213, 244, 274, 305, 335, 366
    };

// em is used in the Gauss algorithm. We store offsets for months 0..12
// for calculating day_of_week. The code in the original Cython used index m
// plus a small adjustment for leap years. We'll store it for direct port.
    static const int EM[13] = {
            0, // unused index 0
            0, 31, 59, 90, 120, 151, 181, 212, 243, 273, 304, 334
    };

    constexpr std::array MONTH {"JAN", "FEB", "MAR", "APR", "MAY", "JUN", "JUL",
                                "AUG", "SEP", "OCT", "NOV", "DEC"};
    constexpr std::array MONTHS_FULL{"", "January", "February", "March", "April", "May", "June",
                                     "July", "August", "September", "October", "November",
                                     "December"};
    constexpr std::array DAYS {"MON", "TUE", "WED", "THU", "FRI", "SAT", "SUN"};
    constexpr std::array DAYS_FULL{"Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday", "Sunday"};

    struct IsoCalendar {
        int year;
        int week;
        int day;
    };

    bool is_leapyear(int64_t year);
    int day_of_week(int y, int m, int d) noexcept;

    int32_t get_firstbday(int year, int month) noexcept;
    int32_t get_lastbday(int year, int month) noexcept;
    int32_t get_day_of_year(int year, int month, int day) noexcept;

    IsoCalendar get_iso_calendar(int year, int month, int day) noexcept;

    int32_t get_week_of_year(int year, int month, int day) noexcept;
    int32_t get_days_in_month(int year, int month) noexcept;
}
