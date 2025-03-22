#include "np_busdaycal.h"
#include <common/python_utils.h>


namespace epoch_frame::np
{
    int get_day_of_week(DateTime const& date)
    {
        return static_cast<int>(date.weekday());
    }

    void normalize_holiday_list(HolidayList& holidays, WeekMask& weekmask)
    {
        int64_t trimcount = 0;

        std::ranges::sort(holidays);
        auto lastdate = holidays.back();

        for (size_t i = 0; i < holidays.size(); ++i) {
            auto date = holidays[i];
            if (date == lastdate) {
                continue;
            }

            const auto day_of_week = get_day_of_week(date);
            if (weekmask[day_of_week]) {
              holidays[trimcount++] = date;
              lastdate = date;
            }
        }

        holidays.assign(holidays.begin(), holidays.begin() + trimcount);
    }

    auto find_earliest_holiday_on_or_after(std::optional<DateTime> const& date, auto const& holidays_begin, auto const& holidays_end)
    {
        return std::lower_bound(holidays_begin, holidays_end, date);
    }

    auto find_earliest_holiday_after(std::optional<DateTime> const& date, auto const& holidays_begin, auto const& holidays_end)
    {
        return std::upper_bound(holidays_begin, holidays_end, date);
    }

    bool is_holiday(std::optional<DateTime> const& date, auto const& holidays_begin, auto const& holidays_end)
    {
        return find_earliest_holiday_on_or_after(date, holidays_begin, holidays_end) != holidays_end;
    }

    bool is_holiday(auto const& iter, auto const& holidays_end)
    {
        return iter != holidays_end;
    }

    void validate_busdays_in_week(int8_t busdays_in_weekmask)
    {
        AssertFromFormat(busdays_in_weekmask > 0, "the business day weekmask must have at least one valid business day");
    }

    int8_t apply_business_day_roll(DateTime date, DateTime& out, BusDayOffsetRoll roll, WeekMask const& weekmask, auto holidays_begin, auto holidays_end)
    {
        auto day_of_week = get_day_of_week(date);
        if (weekmask[day_of_week] == 0 || is_holiday(date, holidays_begin, holidays_end))
        {
            auto start_date = date;
            int start_day_of_week = day_of_week;

            switch (roll)
            {
                case BusDayOffsetRoll::Following:
                case BusDayOffsetRoll::ModifiedFollowing:
                {
                    do {
                        ++date;
                        if (++day_of_week == 7)
                        {
                            day_of_week = 0;
                        }
                    } while (weekmask[day_of_week] == 0 || is_holiday(date, holidays_begin, holidays_end));

                    if (roll == BusDayOffsetRoll::ModifiedFollowing)
                    {
                        if (start_date.date.month != date.date.month)
                        {
                            date = start_date;
                            day_of_week = start_day_of_week;

                            do {
                                ++date;
                                if (--day_of_week == -1)
                                {
                                    day_of_week = 6;
                                }
                            } while (weekmask[day_of_week] == 0 || is_holiday(date, holidays_begin, holidays_end));
                        }
                    }
                    break;
                }
                case BusDayOffsetRoll::Preceding:
                case BusDayOffsetRoll::ModifiedPreceding:
                {
                    do {
                        --date;
                        if (--day_of_week == -1)
                        {
                            day_of_week = 6;
                        }
                    } while (weekmask[day_of_week] == 0 || is_holiday(date, holidays_begin, holidays_end));

                    if (roll == BusDayOffsetRoll::ModifiedPreceding)
                    {
                        if (start_date.date.month != date.date.month)
                        {
                            date = start_date;
                            day_of_week = start_day_of_week;

                            do {
                                --date;
                                if (++day_of_week == 7)
                                {
                                    day_of_week = 0;
                                }
                            } while (weekmask[day_of_week] == 0 || is_holiday(date, holidays_begin, holidays_end));
                        }
                    }
                    break;
                }
                default:
                    break;
            }
        }

        out = date;
        return day_of_week;
    }

    DateTime apply_business_day_offset(DateTime date, int64_t offset, BusDayOffsetRoll roll, WeekMask const& weekmask, uint8_t busdays_in_weekmask, auto holidays_begin, auto holidays_end)
    {
        int day_of_week = apply_business_day_roll(date, date, roll, weekmask, holidays_begin, holidays_end);

        if (offset > 0) {
            holidays_begin = find_earliest_holiday_on_or_after(date, holidays_begin, holidays_end);

            date += ((offset / busdays_in_weekmask) * 7);
            offset %= busdays_in_weekmask;

            auto holidays_temp = find_earliest_holiday_after(date, holidays_begin, holidays_end);
            offset += std::distance(holidays_begin, holidays_temp);
            holidays_begin = holidays_temp;

            while (offset > 0) {
                ++date;
                if (++day_of_week == 7) {
                    day_of_week = 0;
                }

                if (weekmask[day_of_week] && !is_holiday(date, holidays_begin, holidays_end)) {
                    --offset;
                }
            }
        }
        else if (offset < 0) {
            holidays_end = find_earliest_holiday_after(date, holidays_begin, holidays_end);

            date += ((offset / busdays_in_weekmask) * 7);
            offset %= busdays_in_weekmask;

            auto holidays_temp = find_earliest_holiday_after(date, holidays_begin, holidays_end);
            offset -= std::distance(holidays_temp, holidays_end);
            holidays_end = holidays_temp;

            while (offset < 0) {
                --date;
                if (--day_of_week == -1) {
                    day_of_week = 6;
                }

                if (weekmask[day_of_week] && !is_holiday(date, holidays_begin, holidays_end)) {
                    ++offset;
                }
            }
        }

        return date;
    }

    int64_t apply_business_day_count(DateTime date_begin, DateTime date_end, WeekMask const& weekmask, uint8_t busdays_in_weekmask, auto holidays_begin, auto holidays_end)
    {
       int64_t count = 0;
       int64_t whole_weeks = 0;
       int8_t day_of_week = 0;
       int8_t swapped = 0;

       if (date_begin == date_end)
       {
            return 0;
       }

       if (date_begin > date_end)
       {
            auto tmp = date_begin;
            date_begin = date_end;
            date_end = tmp;
            swapped = 1;

            ++date_begin;
            ++date_end;
       }

       holidays_begin = find_earliest_holiday_on_or_after(date_begin, holidays_begin, holidays_end);
       holidays_end = find_earliest_holiday_on_or_after(date_end, holidays_begin, holidays_end);

       count = -(std::distance(holidays_begin, holidays_end));

       whole_weeks = floor_div((date_end - date_begin).days(), 7);
       count += whole_weeks * busdays_in_weekmask;

       date_begin += (whole_weeks * 7);

       if (date_begin < date_end)
       {
            day_of_week = get_day_of_week(date_begin);

            while (date_begin < date_end)
            {
                if (weekmask[day_of_week])
                {
                    ++count;
                }

                ++date_begin;
                if (++day_of_week == 7)
                {
                    day_of_week = 0;
                }
            }
       }

       if (swapped)
       {
            count = -count;
       }

       return count;
    }

    HolidayList business_day_offset(HolidayList const& dates, std::vector<int64_t> const& offsets, BusDayOffsetRoll roll, WeekMask const& weekmask, uint8_t busdays_in_weekmask, HolidayList const& holidays)
    {
        validate_busdays_in_week(busdays_in_weekmask);

        HolidayList result(dates.size());
        std::transform(dates.begin(), dates.end(), offsets.begin(), result.begin(), [&](auto const& date, auto const& offset) {
            return apply_business_day_offset(date, offset, roll, weekmask, busdays_in_weekmask, holidays.begin(), holidays.end());
        });

        return result;
    }

    std::vector<int64_t> business_day_count(HolidayList const& dates_begin, HolidayList const& dates_end, WeekMask const& weekmask, uint8_t busdays_in_weekmask, HolidayList const& holidays)
    {
        validate_busdays_in_week(busdays_in_weekmask);

        std::vector<int64_t> result(dates_begin.size());
        std::transform(dates_begin.begin(), dates_begin.end(), dates_end.begin(), result.begin(), [&](auto const& _date_begin, auto const& _date_end) {
            return apply_business_day_count(_date_begin, _date_end, weekmask, busdays_in_weekmask, holidays.begin(), holidays.end());
        });

        return result;
    }

    bool apply_is_business_day(DateTime const& date, WeekMask const& weekmask, uint8_t busdays_in_weekmask, HolidayList const& holidays)
    {
        auto day_of_week = get_day_of_week(date);
        return weekmask[day_of_week] && !is_holiday(date, holidays.begin(), holidays.end());
    }

    std::vector<bool> is_business_day(HolidayList const& dates, WeekMask const& weekmask, uint8_t busdays_in_weekmask, HolidayList const& holidays)
    {
        validate_busdays_in_week(busdays_in_weekmask);

        std::vector<bool> result(dates.size());
        std::transform(dates.begin(), dates.end(), result.begin(), [&](DateTime const& date) {
            return apply_is_business_day(date, weekmask, busdays_in_weekmask, holidays);
        });

        return result;
    }

    BusinessDayCalendar::BusinessDayCalendar(WeekMask const& weekmask, HolidayList const& holidays) : m_weekmask(weekmask), m_holidays(holidays) {
        m_busdays_in_weekmask = std::accumulate(weekmask.begin(), weekmask.end(), 0, std::plus<bool>());
        AssertFromFormat(m_busdays_in_weekmask > 0, "Cannot construct a busdaycal with a weekmask of all false values");
    }

    HolidayList BusinessDayCalendar::offset(std::vector<DateTime> const& dates, std::vector<int64_t> const& offsets, BusDayOffsetRoll roll) {
        return business_day_offset(dates, offsets, roll, m_weekmask, m_busdays_in_weekmask, m_holidays);
    }

    DateTime BusinessDayCalendar::offset(DateTime const& date, int64_t offset, BusDayOffsetRoll roll) {
        return apply_business_day_offset(date, offset, roll, m_weekmask, m_busdays_in_weekmask, m_holidays.begin(), m_holidays.end());
    }

    std::vector<int64_t> BusinessDayCalendar::count(HolidayList const& dates_begin, HolidayList const& dates_end) {
        return business_day_count(dates_begin, dates_end, m_weekmask, m_busdays_in_weekmask, m_holidays);
    }

    std::vector<bool> BusinessDayCalendar::is_busday(HolidayList const& dates) {
        return is_business_day(dates, m_weekmask, m_busdays_in_weekmask, m_holidays);
    }

    bool BusinessDayCalendar::is_busday(DateTime const& date) {
        return apply_is_business_day(date, m_weekmask, m_busdays_in_weekmask, m_holidays);
    }

    WeekMask to_weekmask(WeekSet const& weekmask) {
        WeekMask result;
        for (auto const& day : weekmask) {
            result[static_cast<uint8_t>(day)] = true;
        }
        return result;
    }

} // namespace epoch_frame::np
