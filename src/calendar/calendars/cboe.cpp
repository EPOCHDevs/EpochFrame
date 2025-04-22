#include "../holidays/us.h"
#include "all.h"
#include "date_time/holiday/holiday.h"
#include "epoch_frame/datetime.h"
#include "epoch_frame/index.h"

#define us USHolidays::Instance()

namespace epoch_frame::calendar
{
    struct CBOEOptions
    {
        /**
         * Custom Good Friday holiday rule
         * Good Friday is a valid trading day if Christmas Day or New Years Day fall on a Friday.
         */
        inline static const auto good_friday_unless_christmas_nye_friday =
            [](const DateTime& dt) -> std::optional<DateTime>
        {
            const auto year = dt.date().year;

            // Check if Christmas falls on a Friday (weekday 4)
            auto christmas = us.Christmas.observance(DateTime{year, std::chrono::December, 25d});
            if (!christmas)
            {
                return std::nullopt;
            }

            auto nyd = us.USNewYearsDay.observance(DateTime{year, std::chrono::January, 1d});
            if (!nyd)
            {
                return std::nullopt;
            }

            bool christmas_on_friday = (christmas->weekday() == 4);

            // Check if New Year's Day falls on a Friday (weekday 4)
            bool nye_on_friday = (nyd->weekday() == 4);

            // If neither falls on Friday, use Good Friday
            if (!christmas_on_friday && !nye_on_friday)
            {
                auto dates = Holiday{GoodFriday}.dates(
                    DateTime{dt.date().year, std::chrono::January, 1d}.timestamp(),
                    DateTime{dt.date().year, std::chrono::December, 31d}.timestamp());
                if (dates->size() > 0)
                {
                    return dates->at(0).to_datetime();
                }
            }

            // Not a holiday, return invalid date
            return std::nullopt;
        };

        const HolidayData GoodFridayUnlessChristmasNYEFriday = {
            .name       = "Good Friday CFE",
            .month      = std::chrono::January,
            .day        = 1d,
            .observance = good_friday_unless_christmas_nye_friday,
        };

        const MarketCalendarOptions CFE_OPTIONS{
            .name                 = "CFE",
            .regular_market_times = RegularMarketTimes{{
                {epoch_core::MarketTimeType::MarketOpen, {MarketTime{Time{8h, 30min}}}},
                {epoch_core::MarketTimeType::MarketClose, {MarketTime{Time{15h, 15min}}}},
            }},
            .tz                   = CST,
            .regular_holidays     = make_unnamed_calendar(
                {us.USNewYearsDay, us.USMartinLutherKingJrAfter1998, us.USPresidentsDay,
                     GoodFridayUnlessChristmasNYEFriday, us.USJuneteenthAfter2022, us.USIndependenceDay,
                     us.USMemorialDay, USLaborDay, us.USThanksgivingDay, us.Christmas},
                DateTime{1900y, January, 1d}),
            .adhoc_holidays = chain(us.HurricaneSandyClosings, us.USNationalDaysofMourning),
            .aliases        = {"CFE", "CBOE_Futures"},
            .weekmask = {epoch_core::EpochDayOfWeek::Monday, epoch_core::EpochDayOfWeek::Tuesday,
                         epoch_core::EpochDayOfWeek::Wednesday,
                         epoch_core::EpochDayOfWeek::Thursday, epoch_core::EpochDayOfWeek::Friday},
            .special_closes = {SpecialTime{Time{.hour = 12h, .minute = 15min},
                                           make_unnamed_calendar({us.USBlackFridayInOrAfter1993},
                                                                 DateTime{1900y, January, 1d})}}};

        const MarketCalendarOptions CBOE_EQUITY_OPTIONS{
            .name                 = "CBOE_Equity_Options",
            .regular_market_times = RegularMarketTimes{{
                {epoch_core::MarketTimeType::MarketOpen, {MarketTime{Time{8h, 30min}}}},
                {epoch_core::MarketTimeType::MarketClose, {MarketTime{Time{15h}}}},
            }},
            .tz                   = CST,
            .regular_holidays     = CFE_OPTIONS.regular_holidays,
            .adhoc_holidays       = CFE_OPTIONS.adhoc_holidays,
            .aliases              = {"CBOE_Equity_Options"},
            .weekmask             = CFE_OPTIONS.weekmask,
            .special_closes       = CFE_OPTIONS.special_closes};

        const MarketCalendarOptions CBOE_INDEX_OPTIONS{
            .name                 = "CBOE_Index_Options",
            .regular_market_times = RegularMarketTimes{{
                {epoch_core::MarketTimeType::MarketOpen, {MarketTime{Time{8h, 30min}}}},
                {epoch_core::MarketTimeType::MarketClose, {MarketTime{Time{15h, 15min}}}},
            }},
            .tz                   = CST,
            .regular_holidays     = CFE_OPTIONS.regular_holidays,
            .adhoc_holidays       = CFE_OPTIONS.adhoc_holidays,
            .aliases              = {"CBOE_Index_Options"},
            .weekmask             = CFE_OPTIONS.weekmask,
            .special_closes       = CFE_OPTIONS.special_closes};
    };

    const CBOEOptions& cboe_instance()
    {
        static CBOEOptions options;
        return options;
    }

    // CFE Exchange Calendar
    CFEExchangeCalendar::CFEExchangeCalendar(std::optional<MarketTime> const& open_time,
                                             std::optional<MarketTime> const& close_time)
        : MarketCalendar(open_time, close_time, cboe_instance().CFE_OPTIONS)
    {
    }

    // CBOE Equity Options Exchange Calendar
    CBOEEquityOptionsExchangeCalendar::CBOEEquityOptionsExchangeCalendar(
        std::optional<MarketTime> const& open_time, std::optional<MarketTime> const& close_time)
        : MarketCalendar(open_time, close_time, cboe_instance().CBOE_EQUITY_OPTIONS)
    {
    }

    // CBOE Index Options Exchange Calendar
    CBOEIndexOptionsExchangeCalendar::CBOEIndexOptionsExchangeCalendar(
        std::optional<MarketTime> const& open_time, std::optional<MarketTime> const& close_time)
        : MarketCalendar(open_time, close_time, cboe_instance().CBOE_INDEX_OPTIONS)
    {
    }

} // namespace epoch_frame::calendar
