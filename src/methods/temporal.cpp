//
// Created by adesola on 2/13/25.
//

#include "temporal.h"


namespace epochframe {
    template<>
    TemporalOperation<true>::TemporalOperation(arrow::ArrayPtr const &array) : m_data(array) {
        AssertWithTraceFromStream(array != nullptr, "array is nullptr");
        AssertWithTraceFromStream(array->type_id() == arrow::Type::TIMESTAMP, "array is not a timestamp");
    }

    template<>
    TemporalOperation<false>::TemporalOperation(arrow::ScalarPtr const &scalar) : m_data(scalar) {
        AssertWithTraceFromStream(scalar != nullptr, "scalar is nullptr");
        AssertWithTraceFromStream(scalar->type->id() == arrow::Type::TIMESTAMP, "scalar is not a timestamp");
    }

    template<>
    IsoCalendarArray TemporalOperation<true>::iso_calendar() const {
        auto result = AssertResultIsOk(arrow::compute::ISOCalendar(m_data)).array_as<arrow::StructArray>();
        auto year = result->GetFieldByName("iso_year");
        AssertWithTraceFromStream(year != nullptr, "year is nullptr");
        auto week = result->GetFieldByName("iso_week");
        AssertWithTraceFromStream(week != nullptr, "week is nullptr");
        auto day_of_week = result->GetFieldByName("iso_day_of_week");
        AssertWithTraceFromStream(day_of_week != nullptr, "day_of_week is nullptr");

        return {
            year,
            week,
            day_of_week
        };
    }

    template<>
    IsoCalendarScalar TemporalOperation<false>::iso_calendar() const {
        auto result = AssertResultIsOk(arrow::compute::ISOCalendar(m_data)).scalar_as<arrow::StructScalar>();
        auto year = AssertResultIsOk(result.field("iso_year"));
        AssertWithTraceFromStream(year != nullptr, "year is nullptr");
        auto week = AssertResultIsOk(result.field("iso_week"));
        AssertWithTraceFromStream(week != nullptr, "week is nullptr");
        auto day_of_week = AssertResultIsOk(result.field("iso_day_of_week"));
        AssertWithTraceFromStream(day_of_week != nullptr, "day_of_week is nullptr");

        return {
            year,
            week,
            day_of_week
        };
    }

    template<>
    YearMonthDayArray TemporalOperation<true>::year_month_day() const {
        auto result = AssertResultIsOk(arrow::compute::YearMonthDay(m_data)).array_as<arrow::StructArray>();
        auto year = result->GetFieldByName("year");
        AssertWithTraceFromStream(year != nullptr, "year is nullptr");
        auto month = result->GetFieldByName("month");
        AssertWithTraceFromStream(month != nullptr, "month is nullptr");
        auto day = result->GetFieldByName("day");
        AssertWithTraceFromStream(day != nullptr, "day is nullptr");

        return {
            year,
            month,
            day
        };
    }

    template<>
    YearMonthDayScalar TemporalOperation<false>::year_month_day() const {
        auto result = AssertResultIsOk(arrow::compute::YearMonthDay(m_data)).scalar_as<arrow::StructScalar>();
        auto year = AssertResultIsOk(result.field("year"));
        AssertWithTraceFromStream(year != nullptr, "year is nullptr");
        auto month = AssertResultIsOk(result.field("month"));
        AssertWithTraceFromStream(month != nullptr, "month is nullptr");
        auto day = AssertResultIsOk(result.field("day"));
        AssertWithTraceFromStream(day != nullptr, "day is nullptr");
        return {
            year, month, day
        };
    }
} // namespace epochframe
