#include "../holidays/us.h"
#include "all.h"
#include "date_time/holiday/holiday.h"

#define us USHolidays::Instance()

namespace epoch_frame::calendar
{
    struct FXOptions
    {
        const MarketCalendarOptions FX_OPTIONS{
            .name = "FX",
            .regular_market_times =
                RegularMarketTimes{{epoch_core::MarketTimeType::MarketOpen,
                                    MarketTimes{MarketTime{Time{17h, 0min, 0s}, -1}}},
                                   {epoch_core::MarketTimeType::MarketClose,
                                    MarketTimes{MarketTime{Time{17h, 0min, 0s}}}}},
            .tz = EST,
            // Forex markets generally only close for a few major holidays
            .regular_holidays = make_unnamed_calendar({us.Christmas, us.USNewYearsDay}),
            .aliases          = {"FX", "Forex", "FX_Market", "Currency"},
            // Include Sunday in weekmask since we handle special opens
            .weekmask = {epoch_core::EpochDayOfWeek::Monday,
                         epoch_core::EpochDayOfWeek::Tuesday, epoch_core::EpochDayOfWeek::Wednesday,
                         epoch_core::EpochDayOfWeek::Thursday, epoch_core::EpochDayOfWeek::Friday}};
    };

    const FXOptions& fx_instance()
    {
        static FXOptions options;
        return options;
    }

    // FX Exchange Calendar
    FXExchangeCalendar::FXExchangeCalendar(std::optional<MarketTime> const& open_time,
                                           std::optional<MarketTime> const& close_time)
        : MarketCalendar(open_time, close_time, fx_instance().FX_OPTIONS)
    {
    }

} // namespace epoch_frame::calendar
