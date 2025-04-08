#include "../holidays/cme.h"
#include "../holidays/us.h"
#include "all.h"
#include "date_time/holiday/holiday.h"

#define us USHolidays::Instance()

namespace epoch_frame::calendar
{
    struct CMEOptions
    {
        const MarketCalendarOptions CME_EQUITY_OPTIONS{
            .name                 = "CME_Equity",
            .regular_market_times = RegularMarketTimes{{
                {epoch_core::MarketTimeType::MarketOpen, {MarketTime{Time{17h}, -1}}},
                {epoch_core::MarketTimeType::MarketClose, {MarketTime{Time{16h}}}},
                {epoch_core::MarketTimeType::BreakStart, {MarketTime{Time{15h, 15min}}}},
                {epoch_core::MarketTimeType::BreakEnd, {MarketTime{Time{15h, 30min}}}},
            }},
            .tz                   = CST,
            .regular_holidays = make_unnamed_calendar({us.USNewYearsDay, GoodFriday, us.Christmas}),
            .adhoc_holidays   = us.USNationalDaysofMourning,
            .aliases          = {"CME_Equity", "CBOT_Equity"},
            .weekmask = {epoch_core::EpochDayOfWeek::Monday, epoch_core::EpochDayOfWeek::Tuesday,
                         epoch_core::EpochDayOfWeek::Wednesday,
                         epoch_core::EpochDayOfWeek::Thursday, epoch_core::EpochDayOfWeek::Friday},
            .special_closes = {SpecialTime{
                Time{.hour = 12h, .tz = CST},
                make_unnamed_calendar({us.USMartinLutherKingJrAfter1998, us.USPresidentsDay,
                                       us.USMemorialDay, USLaborDay, us.USJuneteenthAfter2022,
                                       us.USIndependenceDay, us.USThanksgivingDay,
                                       us.USBlackFridayInOrAfter1993, us.ChristmasEveBefore1993,
                                       us.ChristmasEveInOrAfter1993},
                                      DateTime{1900y, January, 1d})}}};

        const MarketCalendarOptions CME_AGRICULTURE_OPTIONS{
            .name                 = "CME_Agriculture",
            .regular_market_times = RegularMarketTimes{{
                {epoch_core::MarketTimeType::MarketOpen, {MarketTime{Time{17h, 1min}, -1}}},
                {epoch_core::MarketTimeType::MarketClose, {MarketTime{Time{17h}}}},
            }},
            .tz                   = CST,
            .regular_holidays     = make_unnamed_calendar(
                {us.USNewYearsDay, us.USMartinLutherKingJrAfter1998, us.USPresidentsDay, GoodFriday,
                     us.USMemorialDay, us.USJuneteenthAfter2022, us.USIndependenceDay, USLaborDay,
                     us.USThanksgivingDay, us.Christmas}),
            .adhoc_holidays = us.USNationalDaysofMourning,
            .aliases        = {"CME_Agriculture", "CBOT_Agriculture", "COMEX_Agriculture",
                               "NYMEX_Agriculture"},
            .weekmask = {epoch_core::EpochDayOfWeek::Monday, epoch_core::EpochDayOfWeek::Tuesday,
                         epoch_core::EpochDayOfWeek::Wednesday,
                         epoch_core::EpochDayOfWeek::Thursday, epoch_core::EpochDayOfWeek::Friday},
            .special_closes = {SpecialTime{
                Time{.hour = 12h, .tz = CST},
                make_unnamed_calendar({us.USBlackFridayInOrAfter1993, us.ChristmasEveBefore1993,
                                       us.ChristmasEveInOrAfter1993})}} };

        // Good Friday open/closed dates for Bond calendar
        const std::vector<DateTime> BONDS_GOOD_FRIDAY_CLOSED = {
            "1970-03-27"__date, "1971-04-09"__date, "1972-03-31"__date, "1973-04-20"__date,
            "1974-04-12"__date, "1975-03-28"__date, "1976-04-16"__date, "1977-04-08"__date,
            "1978-03-24"__date, "1979-04-13"__date, "1981-04-17"__date, "1982-04-09"__date,
            "1984-04-20"__date, "1986-03-28"__date, "1987-04-17"__date, "1989-03-24"__date,
            "1990-04-13"__date, "1991-03-29"__date, "1992-04-17"__date, "1993-04-09"__date,
            "1995-04-14"__date, "1997-03-28"__date, "1998-04-10"__date, "2000-04-21"__date,
            "2001-04-13"__date, "2002-03-29"__date, "2003-04-18"__date, "2004-04-09"__date,
            "2005-03-25"__date, "2006-04-14"__date, "2008-03-21"__date, "2009-04-10"__date,
            "2011-04-22"__date, "2013-03-29"__date, "2014-04-18"__date, "2016-03-25"__date,
            "2017-04-14"__date, "2018-03-30"__date, "2019-04-19"__date, "2020-04-10"__date,
            "2022-04-15"__date, "2024-03-29"__date, "2025-04-18"__date};

        const std::vector<DateTime> BONDS_GOOD_FRIDAY_OPEN = {
            "1980-04-04"__date, "1983-04-01"__date, "1985-04-05"__date, "1988-04-01"__date,
            "1994-04-01"__date, "1996-04-05"__date, "1999-04-02"__date, "2007-04-06"__date,
            "2010-04-02"__date, "2012-04-06"__date, "2015-04-03"__date, "2021-04-02"__date,
            "2023-04-07"__date};

        const MarketCalendarOptions CME_BOND_OPTIONS{
            .name                 = "CME_Bond",
            .regular_market_times = RegularMarketTimes{{
                {epoch_core::MarketTimeType::MarketOpen, {MarketTime{Time{17h}, -1}}},
                {epoch_core::MarketTimeType::MarketClose, {MarketTime{Time{16h}}}},
            }},
            .tz                   = CST,
            .regular_holidays     = make_unnamed_calendar({us.USNewYearsDay, us.Christmas}),
            .adhoc_holidays       = chain(us.USNationalDaysofMourning, BONDS_GOOD_FRIDAY_CLOSED),
            .aliases  = {"CME_Rate", "CBOT_Rate", "CME_InterestRate", "CBOT_InterestRate",
                         "CME_Bond", "CBOT_Bond"},
            .weekmask = {epoch_core::EpochDayOfWeek::Monday, epoch_core::EpochDayOfWeek::Tuesday,
                         epoch_core::EpochDayOfWeek::Wednesday,
                         epoch_core::EpochDayOfWeek::Thursday, epoch_core::EpochDayOfWeek::Friday},
            .special_closes       = {SpecialTime{
                                   Time{.hour = 12h, .tz = CST},
                                   make_unnamed_calendar({us.USMartinLutherKingJrAfter1998,
                                                                us.USPresidentsDay, us.USMemorialDay,
                                                                us.USIndependenceDay, USLaborDay,
                                                                us.USThanksgivingDay},
                                                               DateTime{1900y, January, 1d})},
                                     SpecialTime{Time{.hour = 12h, .minute = 15min, .tz = CST},
                                           make_unnamed_calendar({us.USBlackFridayInOrAfter1993,
                                                                        us.ChristmasEveBefore1993,
                                                                        us.ChristmasEveInOrAfter1993},
                                                                       DateTime{1900y, January, 1d})}},
            .special_closes_adhoc = {SpecialTimeAdHoc{
                .time     = Time{.hour = 10h, .tz = CST},
                .calendar = factory::index::make_datetime_index(BONDS_GOOD_FRIDAY_OPEN)}}};
    };

    const CMEOptions& cme_instance()
    {
        static CMEOptions options;
        return options;
    }

    // CME Equity Exchange Calendar
    CMEEquityExchangeCalendar::CMEEquityExchangeCalendar(
        std::optional<MarketTime> const& open_time, std::optional<MarketTime> const& close_time)
        : MarketCalendar(open_time, close_time, cme_instance().CME_EQUITY_OPTIONS)
    {
    }

    // CME Agriculture Exchange Calendar
    CMEAgricultureExchangeCalendar::CMEAgricultureExchangeCalendar(
        std::optional<MarketTime> const& open_time, std::optional<MarketTime> const& close_time)
        : MarketCalendar(open_time, close_time, cme_instance().CME_AGRICULTURE_OPTIONS)
    {
    }

    // CME Bond Exchange Calendar
    CMEBondExchangeCalendar::CMEBondExchangeCalendar(std::optional<MarketTime> const& open_time,
                                                     std::optional<MarketTime> const& close_time)
        : MarketCalendar(open_time, close_time, cme_instance().CME_BOND_OPTIONS)
    {
    }

} // namespace epoch_frame::calendar
