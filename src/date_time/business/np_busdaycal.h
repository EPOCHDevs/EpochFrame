#pragma once

#include "date_time/datetime.h"
#include <unordered_set>
#include "date_time/day_of_week.h"


namespace epoch_frame::np{


    enum class BusDayOffsetRoll{
        Following=0,
        Preceding=1,
        ModifiedFollowing=3,
        ModifiedPreceding=4,
        NAT=5,
        Raise=6
    };

    using HolidayList = std::vector<DateTime>;
    using WeekMask = std::array<bool, 7>;
    using WeekSet = std::unordered_set<epoch_core::EpochDayOfWeek>;

    WeekMask to_weekmask(WeekSet const& weekmask);

    class BusinessDayCalendar{
        public:
            BusinessDayCalendar(WeekMask const& weekmask, HolidayList const& holidays);
            WeekMask weekmask() const noexcept { return m_weekmask; }
            HolidayList holidays() const noexcept { return m_holidays; }
            int8_t busdays_in_weekmask() const noexcept { return m_busdays_in_weekmask; }

            HolidayList offset(std::vector<DateTime> const& dates, std::vector<int64_t> const& offsets, BusDayOffsetRoll roll=BusDayOffsetRoll::Raise);
            DateTime offset(DateTime const& date, int64_t offset, BusDayOffsetRoll roll=BusDayOffsetRoll::Raise);
            std::vector<int64_t> count(HolidayList const& dates_begin, HolidayList const& dates_end);
            int64_t count(DateTime const& dates_begin, DateTime const& dates_end);
            std::vector<bool> is_busday(HolidayList const& dates);
            bool is_busday(DateTime const& date);

      private:
        WeekMask m_weekmask{true, true, true, true, true, false, false};
        HolidayList m_holidays;
        int8_t m_busdays_in_weekmask{5};
    };

    using BusinessDayCalendarPtr = std::shared_ptr<BusinessDayCalendar>;
    const BusinessDayCalendarPtr DEFAULT_BUSDAYCAL = std::make_shared<BusinessDayCalendar>(WeekMask{true, true, true, true, true, false, false}, HolidayList{});
}
