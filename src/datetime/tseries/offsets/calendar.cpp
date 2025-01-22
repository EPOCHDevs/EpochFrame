//
// Created by adesola on 1/21/25.
//

#include "calendar.h"
#include <algorithm>


namespace epochframe::datetime {

// ----------------------------------------------------------------------
// Implementation

            bool is_leap_year(int64_t year) noexcept {
                return ((year & 0x3) == 0 and //# year % 4 == 0
                        ((year % 100) != 0 or (year % 400) == 0));
            }

            int32_t get_days_in_month(int year, int month) noexcept {
                return DAYS_PER_MONTH_ARRAY[12 * is_leapyear(year) + month - 1];
            }

/**
 * A helper function that mimics the "quot" function from the Cython code,
 * which does integer floor division with negative adjustments.
 *
 * In modern C++, signed integer division always truncs toward 0,
 * so we replicate the logic that if (a < 0 && a % b != 0), we do x -= 1.
 */
             long quot(long a, long b) noexcept {
        long x = a / b;
        if (a < 0)
            x -= (a % b != 0);
        return x;
    }

            int day_of_week(int y, int m, int d) noexcept {
                if (m < 3) y -= 1;

                auto c = quot(y, 100);
                auto g = y - c * 100;
                auto f = 5 * (c - quot(c, 4) * 4);
                auto e = EM[m];

                if (m > 2) e -= 1;
                return (-1 + d + e + f + g + g/4) % 7;
            }

            int32_t get_day_of_year(int year, int month, int day) noexcept {
                // We use the MONTH_OFFSET array:
                //   index = (isLeap ? 13 : 0) + (month -1)
                //   result = month_offset[idx] + day
                bool leap = is_leap_year(year);
                int idx = (leap ? 13 : 0) + (month - 1);
                return MONTH_OFFSET[idx] + day;
            }

            IsoCalendar get_iso_calendar(int year, int month, int day) noexcept {
                // Original code:
                //  doy = get_day_of_year(year,month,day)
                //  dow = day_of_week(year,month,day)  # 0=Mon..6=Sun
                //  iso_week = (doy-1) - dow + 3
                //  if iso_week >= 0: iso_week = iso_week//7 + 1
                //  ...
                //  day = dow + 1  # i.e. 1=Mon..7=Sun
                //  then handle out-of-range iso_week with checks for 52/53
                //  adjust iso_year if iso_week=1 in DEC => year+1
                //              or iso_week=52/53 in Jan => year-1
                IsoCalendar result;
                int32_t doy = get_day_of_year(year, month, day);
                int dow = day_of_week(year, month, day);  // 0=Monday
                // We define iso_week in a few steps
                int iso_week = (doy - 1) - dow + 3;  // guess
                if (iso_week >= 0) {
                    iso_week = iso_week / 7 + 1;
                }

                // verify
                if (iso_week < 0) {
                    // if iso_week > -2 or iso_week == -2 and is_leapyear(year-1) => 53
                    if ((iso_week > -2) || (iso_week == -2 && is_leap_year(year - 1))) {
                        iso_week = 53;
                    } else {
                        iso_week = 52;
                    }
                } else if (iso_week == 53) {
                    // if (31 - day + dow < 3), set iso_week = 1
                    if ((31 - day + dow) < 3) {
                        iso_week = 1;
                    }
                }

                // iso_year can differ from 'year'
                int iso_year = year;
                // if iso_week=1 and month=12 => iso_year+1
                if (iso_week == 1 && month == 12) {
                    iso_year += 1;
                }
                    // if iso_week>=52 and month=1 => iso_year-1
                else if (iso_week >= 52 && month == 1) {
                    iso_year -= 1;
                }

                result.year = iso_year;
                result.week = iso_week;
                // day in [1..7], so dow+1
                result.day = dow + 1;
                return result;
            }

            int32_t get_week_of_year(int year, int month, int day) noexcept {
                // Just calls get_iso_calendar and returns the .week field
                IsoCalendar iso = get_iso_calendar(year, month, day);
                return iso.week;
            }

            int get_last_bday(int year, int month) noexcept {
                // from original code:
                //  int wkday = dayofweek(year, month, 1)
                //  int days_in_month = get_days_in_month(...)
                //  return days_in_month - max(((wkday + days_in_month -1) % 7)-4, 0)
                int wkday = day_of_week(year, month, 1);      // 0=Mon..6=Sun
                int dim   = get_days_in_month(year, month);
                // expression: (wkday + dim -1) %7 => day-of-week for the *last* day
                // then -4 => if that is >= 0 => means weekend?
                int val = ((wkday + dim - 1) % 7) - 4;
                int shift = std::max(val, 0);
                return dim - shift;
            }

            int get_first_bday(int year, int month) noexcept {
                // from original code:
                //  int wkday = dayofweek(year, month, 1)
                //  if (wkday == 5) => 1st is Saturday => first=3
                //  elif (wkday == 6) => 1st is Sunday => first=2
                //  else => first=1
                int wkday = day_of_week(year, month, 1);  // 0=Mon..6=Sun
                int first = 1;
                if (wkday == 5) {       // 5=Saturday
                    first = 3;
                } else if (wkday == 6) { // 6=Sunday
                    first = 2;
                }
                return first;
            }

}
