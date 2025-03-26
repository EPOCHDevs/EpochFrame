#pragma once
#include "../calendar_common.h"
#include "../market_calendar.h"

namespace epoch_frame::calendar {

    constexpr const char* EST = "America/New_York";
    constexpr const char* CET = "Europe/London";
    constexpr const char* UTC = "UTC";
    constexpr const char* PST = "America/Los_Angeles";
    constexpr const char* MST = "America/Denver";
    constexpr const char* CST = "America/Chicago";
    constexpr const char* AST = "America/Halifax";
    constexpr const char* HST = "Pacific/Honolulu";

    struct NYSEExhangeCalendar : public MarketCalendar {

        NYSEExhangeCalendar(std::optional<MarketTime> const& open_time,
                       std::optional<MarketTime> const& close_time);

        IndexPtr valid_days(const Date& start_date, const Date& end_date, std::string const& tz = "UTC") const override;

        Series days_at_time(IndexPtr const& days, const std::variant<Time, epoch_core::MarketTimeType>& market_time, int64_t day_offset = 0) const override;

        IndexPtr date_range_htf(Date const& start, Date const& end, std::optional<int64_t> periods = {}) const override;
    };

}
