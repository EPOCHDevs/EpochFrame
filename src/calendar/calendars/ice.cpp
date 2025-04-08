#include "../holidays/us.h"
#include "all.h"
#include "date_time/holiday/holiday.h"

#define us USHolidays::Instance()

namespace epoch_frame::calendar
{
    struct ICEOptions
    {
        const MarketCalendarOptions ICE_OPTIONS{
            .name                 = "ICE",
            .regular_market_times = RegularMarketTimes{{
                {epoch_core::MarketTimeType::MarketOpen, {MarketTime{Time{20h, 1min}, -1}}},
                {epoch_core::MarketTimeType::MarketClose, {MarketTime{Time{18h}}}},
            }},
            .tz                   = EST,
            .regular_holidays = make_unnamed_calendar({us.USNewYearsDay, GoodFriday, us.Christmas},
                                                      DateTime{1900y, January, 1d}),
            .adhoc_holidays   = chain(
                us.USNationalDaysofMourning,
                std::vector<DateTime>{// ICE was only closed on the first day of Hurricane Sandy
                                      "2012-10-29"__date.replace_tz(UTC)}),
            .aliases  = {"ICE", "ICEUS", "NYFE"},
            .weekmask = {epoch_core::EpochDayOfWeek::Monday, epoch_core::EpochDayOfWeek::Tuesday,
                         epoch_core::EpochDayOfWeek::Wednesday,
                         epoch_core::EpochDayOfWeek::Thursday, epoch_core::EpochDayOfWeek::Friday},
            .special_closes = {SpecialTime{
                Time{.hour = 13h},
                make_unnamed_calendar({us.USMartinLutherKingJrAfter1998, us.USPresidentsDay,
                                       us.USMemorialDay, us.USIndependenceDay, USLaborDay,
                                       us.USThanksgivingDay},
                                      DateTime{1900y, January, 1d})}}};
    };

    const ICEOptions& ice_instance()
    {
        static ICEOptions options;
        return options;
    }

    // ICE Exchange Calendar
    ICEExchangeCalendar::ICEExchangeCalendar(std::optional<MarketTime> const& open_time,
                                             std::optional<MarketTime> const& close_time)
        : MarketCalendar(open_time, close_time, ice_instance().ICE_OPTIONS)
    {
    }

} // namespace epoch_frame::calendar