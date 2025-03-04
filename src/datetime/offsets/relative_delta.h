#pragma once
#include <optional>
#include <cmath>
#include <stdexcept>
#include <boost/date_time/posix_time/posix_time.hpp>
#include <boost/date_time/gregorian/gregorian.hpp>

// A simple sign function.
inline int sign(int x) {
    return (x > 0) - (x < 0);
}

namespace epochframe {
    namespace datetime {

        enum class Weekday {
            Monday,
            Tuesday,
            Wednesday,
            Thursday,
            Friday,
            Saturday,
            Sunday
        };

        constexpr auto WEEK_DAYS = {Weekday::Monday, Weekday::Tuesday, Weekday::Wednesday,
                                    Weekday::Thursday, Weekday::Friday, Weekday::Saturday,
                                    Weekday::Sunday};

        // Bring some Boost types into the namespace.
        using boost::posix_time::ptime;

        ///
        /// \brief A structure that holds input parameters for creating a RelativeDelta.
        ///
        /// In this design you can either provide two date/time values (dt1 and dt2)
        /// to compute the delta, or supply relative differences (years, months, days,
        /// weeks, hours, minutes, seconds, microseconds, leapdays). When both dt1 and dt2
        /// are valid (i.e. not a “not_a_date_time” value) the dt1/dt2 branch is used and
        /// the relative differences are ignored.
        ///
        struct RelativeDeltaOption {
            // For computing the difference.
            boost::posix_time::ptime dt1{boost::posix_time::not_a_date_time};
            boost::posix_time::ptime dt2{boost::posix_time::not_a_date_time};

            // Relative differences.
            int years = 0;
            int months = 0;
            double days = 0.0;   // days not counting any weeks
            int weeks = 0;       // additional weeks (will be added as days)
            int hours = 0;
            int minutes = 0;
            int seconds = 0;
            int microseconds = 0;
            int leapdays = 0;    // extra days to add if the resulting date is in a leap year

            // Absolute differences.
            std::optional<int> year;
            std::optional<int> month;
            std::optional<int> day;
            std::optional<int> weekday;
            std::optional<int> hour;
            std::optional<int> minute;
            std::optional<int> second;
            std::optional<int> microsecond;
        };

        ///
        /// \brief The RelativeDelta class represents a relative difference between dates.
        ///
        /// The class stores relative differences in years, months, days, hours, minutes,
        /// seconds, microseconds and leapdays. It overloads arithmetic operators so that
        /// you can add a RelativeDelta to a Boost date/time value, or combine RelativeDelta
        /// objects.
        ///
        class RelativeDelta {
        public:
            // --- Constructors ---
            /// Two-datetime constructor: compute the delta from dt1 and dt2.
            RelativeDelta(boost::posix_time::ptime const& dt1,
                          boost::posix_time::ptime const& dt2);

            /// Construct a RelativeDelta from a RelativeDeltaOption structure.
            /// If both dt1 and dt2 are valid the delta is computed from them.
            explicit RelativeDelta(const RelativeDeltaOption &opt);

            // --- Member Functions ---
            /// Returns the number of whole weeks.
            int weeks() const {
                return static_cast<int>(m_opt.days / 7.0);
            }

            /// Set the weeks (the effect is to adjust the day component).
            void set_weeks(int value) {
                // Remove the old weeks (if any) and add new weeks.
                m_opt.days = m_opt.days - (this->weeks() * 7) + value * 7;
            }

            /// Returns a normalized version of this RelativeDelta where each component is
            /// “carried” into the next largest unit when possible.
            RelativeDelta normalized() const;

            // --- Operator Overloads ---

            /// Add this RelativeDelta to a Boost ptime.
            boost::posix_time::ptime operator+(const boost::posix_time::ptime &dt) const;

            /// Component‐wise addition.
            RelativeDelta operator+(const RelativeDelta &other) const;

            /// Component‐wise subtraction.
            RelativeDelta operator-(const RelativeDelta &other) const;

            /// Negation.
            RelativeDelta operator-() const;

            /// Multiply by a scalar.
            RelativeDelta operator*(double factor) const;

            /// Divide by a scalar.
            RelativeDelta operator/(double divisor) const;

        private:
            // Relative components are stored in this option struct.
            RelativeDeltaOption m_opt;
            bool m_hasTime{}; // (currently not used)

            void set_months(int months);
        };

        ///
        /// \brief Non‐member operator+ to allow ptime + RelativeDelta.
        ///
        boost::posix_time::ptime operator+(const boost::posix_time::ptime& dt, const RelativeDelta& rd);

    } // namespace datetime
} // namespace epochframe
