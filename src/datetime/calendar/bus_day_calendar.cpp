//
// Created by adesola on 2/8/25.
//

#include "bus_day_calendar.h"
#include <algorithm>
#include <cctype>
#include <stdexcept>
#include <epoch_lab_shared/macros.h>


// --- Helper Functions ---
namespace epochframe::datetime {
    WeekMask::WeekMask(const std::string &s)  {
        std::string trimmed;

        for (char c: s) {
            if (!std::isspace(static_cast<unsigned char>(c))) {
                trimmed.push_back(c);
            }
        }
        AssertWithTraceFromStream(s.size() == 7, "weekmask must be 7 characters long");
        for (int i = 0; i < 7; ++i) {
            m_mask[i] = (s[i] == '1');
        }
    }

    WeekMask::WeekMask(const std::array<bool, 7> &weekmask) : m_mask(weekmask) {}

    WeekMask::WeekMask(const std::vector<boost::gregorian::greg_weekday> &weekmask) {
        AssertWithTraceFromStream(weekmask.size() < 7, "weekmask must be less than 7");
        for (auto const &wday: weekmask) {
            m_mask[wday.as_number()] = true;
        }
    }

    int BusDayCalendar::dayOfWeek(boost::gregorian::date date) {
        // Use the convention: (date - 4) mod 7. Adjust negative modulo as needed.
        int dow = static_cast<int>((date.day_of_week().as_number() - 4) % 7);
        if (dow < 0)
            dow += 7;
        return dow;
    }

    std::vector<boost::gregorian::date> BusDayCalendar::normalizeHolidaysList(const std::vector<boost::gregorian::date> &holidays_in,
                                                            const WeekMask &weekmask) {
        std::vector<boost::gregorian::date> norm;
        norm.reserve(holidays_in.size());
        for (boost::gregorian::date const& d: holidays_in) {
            if (d.is_not_a_date())
                continue;
            int dow = dayOfWeek(d);
            if (weekmask[dow])
                norm.push_back(d);
        }
        std::sort(norm.begin(), norm.end());
        norm.erase(std::unique(norm.begin(), norm.end()), norm.end());
        return norm;
    }

// --- Constructors ---

    BusDayCalendar::BusDayCalendar(const WeekMask &wm,
                                   const std::vector<boost::gregorian::date> &holidays_in)
            : weekmask(wm) {
        busdays_in_weekmask = 0;
        for (bool b: weekmask) {
            if (b)
                ++busdays_in_weekmask;
        }
        if (busdays_in_weekmask == 0) {
            throw std::invalid_argument("Weekmask cannot be all zeros");
        }
        setHolidays(holidays_in);
    }

// --- Setters ---
    void BusDayCalendar::setHolidays(const std::vector<boost::gregorian::date> &holidays_in) {
        holidays = normalizeHolidaysList(holidays_in, weekmask);
    }

// --- Getters ---

    const WeekMask &BusDayCalendar::getWeekmask() const {
        return weekmask;
    }

    const HolidayList &BusDayCalendar::getHolidays() const {
        return holidays;
    }

    int BusDayCalendar::getBusinessDaysInWeekmask() const {
        return busdays_in_weekmask;
    }

// --- Business Day Test ---

    bool BusDayCalendar::isBusinessDay(boost::gregorian::date date) const {
        if (date.is_not_a_date())
            return false;
        int dow = dayOfWeek(date);
        if (!weekmask[dow])
            return false;
        // Since the holidays vector is sorted, use binary search.
        return !std::binary_search(holidays.begin(), holidays.end(), date);
    }
}
