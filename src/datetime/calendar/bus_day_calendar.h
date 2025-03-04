#pragma once

#include <array>
#include <vector>
#include <string>
#include <cstdint>
#include "boost/date_time/gregorian/gregorian.hpp"
#include <epoch_lab_shared/macros.h>


namespace epochframe::datetime {
    using HolidayList = std::vector<boost::gregorian::date>;

    class WeekMask {
    public:
        WeekMask() = default;

        WeekMask(const std::string &s);

        WeekMask(const std::array<bool, 7> &weekmask);

        WeekMask(const std::vector<boost::gregorian::greg_weekday> &weekmask);

        bool operator[](int i) const {
            AssertWithTraceFromStream(i >= 0 && i < 7, "Index out of bounds: weekmask index must be between 0 and 6");
            return m_mask[i];
        }

        auto begin() const { return m_mask.begin(); }
        auto end() const { return m_mask.end(); }

    private:
        std::array<bool, 7> m_mask{true, true, true, true, true, false, false};
    };

/// \brief BusDayCalendar encapsulates a business day calendar.
///
/// It stores a weekmask (7 elements, where index 0 represents Monday and 6 Sunday)
/// and a normalized list of holiday dates. The holiday list is normalized by:
///   - Removing any NaT values,
///   - Removing duplicate dates,
///   - Keeping only dates that fall on a business day (according to the weekmask).
///
/// The class provides constructors to set the weekmask (via a string or an array)
/// and the holidays, plus a method to common if a given date is a business day.
    class BusDayCalendar {
    public:
        // --- Constructors ---

        /// \brief Default constructor.
        ///
        /// Sets the weekmask to "1111100" (Monday–Friday are business days)
        /// and uses an empty holiday list.
        BusDayCalendar()=default;

        /// \brief Construct from a 7-element weekmask array and a list of holidays.
        BusDayCalendar(const WeekMask &wm,
                       const HolidayList &holidays_in);

        // --- Setters ---

        /// \brief Set the holidays list.
        ///
        /// The provided holiday dates are normalized (sorted, duplicate-free, and
        /// only those falling on business days are retained).
        void setHolidays(const std::vector<boost::gregorian::date> &holidays_in);

        // --- Getters ---

        /// \brief Return the current weekmask.
        const WeekMask &getWeekmask() const;

        /// \brief Return the normalized holiday list.
        const std::vector<boost::gregorian::date> &getHolidays() const;

        /// \brief Return the number of business days in a week.
        int getBusinessDaysInWeekmask() const;

        // --- Business Day Test ---

        /// \brief Determine if the given date is a business day.
        ///
        /// A date is a business day if it is not NaT, its day-of-week is marked as a business day
        /// in the weekmask, and it is not in the holidays list.
        bool isBusinessDay(boost::gregorian::date date) const;

    private:
        WeekMask weekmask{}; // Monday (index 0) through Sunday (index 6)
        HolidayList holidays;   // normalized (sorted, duplicate-free) list of holidays
        int busdays_in_weekmask{5};      // number of true values in the weekmask

        // --- Helper Functions ---

        /// \brief Compute the day-of-week for a given date.
        ///
        /// Uses the convention that (date - 4) mod 7 gives the day index,
        /// so that a reference date (e.g. 1970-01-05, which equals 4) is a Monday.
        static int dayOfWeek(boost::gregorian::date date);

        /// \brief Normalize a holidays list.
        ///
        /// Removes NaT values, duplicates, and any date that does not fall on a business day
        /// (as determined by the weekmask). Returns the normalized, sorted list.
        static HolidayList
        normalizeHolidaysList(const HolidayList &holidays_in,
                              const WeekMask &weekmask);
    };
}
