//
// Created by adesola on 2/13/25.
//

#pragma once
#include "epochframe/aliases.h"
#include <arrow/compute/api.h>


namespace epochframe {
    class TemporalOperation {
    public:
        TemporalOperation(arrow::ArrayPtr const &);

        arrow::ArrayPtr ceil(arrow::compute::RoundTemporalOptions const &);

        arrow::ArrayPtr floor(arrow::compute::RoundTemporalOptions const &);

        arrow::ArrayPtr round(arrow::compute::RoundTemporalOptions const &);

        arrow::ArrayPtr strftime(arrow::compute::StrftimeOptions const &);

        arrow::ArrayPtr strptime(arrow::compute::StrptimeOptions const &);

        // component

        arrow::ArrayPtr day() const;

        arrow::ArrayPtr day_of_week(arrow::compute::DayOfWeekOptions const &) const;

        arrow::ArrayPtr day_of_year() const;

        arrow::ArrayPtr hour() const;

        arrow::ArrayPtr is_dst() const;

        arrow::ArrayPtr iso_week() const;

        arrow::ArrayPtr iso_year() const;

        arrow::ArrayPtr iso_calendar() const;

        arrow::ArrayPtr is_leap_year() const;

        arrow::ArrayPtr microsecond() const;

        arrow::ArrayPtr millisecond() const;

        arrow::ArrayPtr minute() const;

        arrow::ArrayPtr month() const;

        arrow::ArrayPtr nanosecond() const;

        arrow::ArrayPtr quarter() const;

        arrow::ArrayPtr second() const;

        arrow::ArrayPtr subsecond() const;

        arrow::ArrayPtr us_week() const;

        arrow::ArrayPtr us_year() const;

        arrow::ArrayPtr week(arrow::compute::WeekOptions const &) const;

        arrow::ArrayPtr year() const;

        arrow::ArrayPtr year_month_day() const;

        // difference
        arrow::ArrayPtr day_time_interval_between(arrow::ArrayPtr const &) const;

        arrow::ArrayPtr days_between(arrow::ArrayPtr const &) const;

        arrow::ArrayPtr hours_between(arrow::ArrayPtr const &) const;

        arrow::ArrayPtr microseconds_between(arrow::ArrayPtr const &) const;

        arrow::ArrayPtr milliseconds_between(arrow::ArrayPtr const &) const;

        arrow::ArrayPtr minutes_between(arrow::ArrayPtr const &) const;

        arrow::ArrayPtr month_day_nano_interval_between(arrow::ArrayPtr const &) const;

        arrow::ArrayPtr month_interval_between(arrow::ArrayPtr const &) const;

        arrow::ArrayPtr nanoseconds_between(arrow::ArrayPtr const &) const;

        arrow::ArrayPtr quarters_between(arrow::ArrayPtr const &) const;

        arrow::ArrayPtr seconds_between(arrow::ArrayPtr const &) const;

        arrow::ArrayPtr weeks_between(arrow::ArrayPtr const &, const arrow::compute::DayOfWeekOptions const &) const;

        arrow::ArrayPtr years_between(arrow::ArrayPtr const &) const;

        // timezone handling
        arrow::ArrayPtr assume_timezone(arrow::compute::AssumeTimezoneOptions const&) const;

        arrow::ArrayPtr local_timestamp() const;

    private:
        arrow::ArrayPtr m_data;
    };
}
