//
// Created by adesola on 2/15/25.
//

#pragma once
#include "date_time/date_offsets.h"

namespace epoch_frame::factory::offset
{
    inline std::shared_ptr<BaseCalendarOffsetHandler> nanos(int64_t n)
    {
        return std::make_shared<NanoHandler>(n);
    }

    inline std::shared_ptr<BaseCalendarOffsetHandler> micro(int64_t n)
    {
        return std::make_shared<MicroHandler>(n);
    }

    inline std::shared_ptr<BaseCalendarOffsetHandler> millis(int64_t n)
    {
        return std::make_shared<MilliHandler>(n);
    }

    inline std::shared_ptr<BaseCalendarOffsetHandler> seconds(int64_t n)
    {
        return std::make_shared<SecondHandler>(n);
    }

    inline std::shared_ptr<BaseCalendarOffsetHandler> minutes(int64_t n)
    {
        return std::make_shared<MinuteHandler>(n);
    }

    inline std::shared_ptr<BaseCalendarOffsetHandler> hours(int64_t n)
    {
        return std::make_shared<HourHandler>(n);
    }

    inline std::shared_ptr<BaseCalendarOffsetHandler> days(int64_t n)
    {
        return std::make_shared<DayHandler>(n);
    }

    inline std::shared_ptr<BaseCalendarOffsetHandler>
    weeks(int64_t n, std::optional<epoch_core::EpochDayOfWeek> weekday = std::nullopt)
    {
        return std::make_shared<WeekHandler>(n, weekday);
    }

    // Month offset factory functions
    inline std::shared_ptr<BaseCalendarOffsetHandler> month_start(int64_t n)
    {
        return std::make_shared<MonthStartHandler>(n);
    }

    inline std::shared_ptr<BaseCalendarOffsetHandler> month_end(int64_t n)
    {
        return std::make_shared<MonthEndHandler>(n);
    }

    // Quarter offset factory functions
    inline std::shared_ptr<BaseCalendarOffsetHandler>
    quarter_start(int64_t                           n,
                  std::optional<std::chrono::month> starting_month = std::chrono::January)
    {
        return std::make_shared<QuarterStartHandler>(n, starting_month);
    }

    inline std::shared_ptr<BaseCalendarOffsetHandler>
    quarter_end(int64_t n, std::optional<std::chrono::month> starting_month = std::chrono::December)
    {
        return std::make_shared<QuarterEndHandler>(n, starting_month);
    }

    // Year offset factory functions
    inline std::shared_ptr<BaseCalendarOffsetHandler>
    year_start(int64_t n, std::optional<std::chrono::month> month = std::chrono::January)
    {
        return std::make_shared<YearStartHandler>(n, month);
    }

    inline std::shared_ptr<BaseCalendarOffsetHandler>
    year_end(int64_t n, std::optional<std::chrono::month> month = std::chrono::December)
    {
        return std::make_shared<YearEndHandler>(n, month);
    }

    // RelativeDelta offset factory function for testing
    inline DateOffsetHandlerPtr date_offset(int64_t n, const RelativeDeltaOption& delta)
    {
        return std::make_shared<RelativeDeltaOffsetHandler>(n, delta);
    }

    inline DateOffsetHandlerPtr date_offset(const Weekday& weekday)
    {
        return std::make_shared<RelativeDeltaOffsetHandler>(
            1, RelativeDeltaOption{.weekday = weekday});
    }

    inline DateOffsetHandlerPtr date_offset(const RelativeDeltaOption& delta)
    {
        return std::make_shared<RelativeDeltaOffsetHandler>(1, delta);
    }

    inline DateOffsetHandlerPtr easter_offset(int64_t n = 1)
    {
        return std::make_shared<EasterHandler>(n);
    }

    inline DateOffsetHandlerPtr bday(int64_t                  n         = 1,
                                     std::optional<TimeDelta> timedelta = std::nullopt)
    {
        return std::make_shared<BusinessDay>(n, timedelta);
    }

    inline DateOffsetHandlerPtr cbday(BusinessMixinParams const& params, int64_t n = 1,
                                      std::optional<TimeDelta> timedelta = std::nullopt)
    {
        return std::make_shared<CustomBusinessDay>(params, n, timedelta);
    }

    inline DateOffsetHandlerPtr
    session_anchor(SessionRange session, SessionAnchorWhich which = SessionAnchorWhich::BeforeClose,
                   TimeDelta delta = TimeDelta{{.minutes = 0}}, int64_t n = 1)
    {
        return std::make_shared<SessionAnchorOffsetHandler>(std::move(session), which, delta, n);
    }

    inline DateOffsetHandlerPtr week_of_month(int64_t n, int week,
                                              epoch_core::EpochDayOfWeek weekday)
    {
        if (week < 0 || week > 3)
        {
            throw std::runtime_error("Week must be in range 0..3 for WeekOfMonth");
        }
        return std::make_shared<WeekOfMonthOffsetHandler>(n, week, weekday);
    }

    inline DateOffsetHandlerPtr last_week_of_month(int64_t n, epoch_core::EpochDayOfWeek weekday)
    {
        return std::make_shared<LastWeekOfMonthOffsetHandler>(n, weekday);
    }

    inline DateOffsetHandlerPtr bmonth_begin(int64_t n = 1)
    {
        return std::make_shared<BusinessMonthOffsetHandler>(
            n, BusinessMonthOffsetHandler::BusinessEdge::Begin);
    }

    inline DateOffsetHandlerPtr bmonth_end(int64_t n = 1)
    {
        return std::make_shared<BusinessMonthOffsetHandler>(
            n, BusinessMonthOffsetHandler::BusinessEdge::End);
    }
} // namespace epoch_frame::factory::offset
