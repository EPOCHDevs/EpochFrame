#include "all.h"
#include "date_time/holiday/holiday.h"

namespace epoch_frame::calendar {
    struct CryptoOptions
    {
        const MarketCalendarOptions CRYPTO_OPTIONS{
            .name                 = "Crypto",
            .regular_market_times =
    RegularMarketTimes{{epoch_core::MarketTimeType::MarketOpen,
                        MarketTimes{MarketTime{Time{0h, 0min, 0s}}}},
                       {epoch_core::MarketTimeType::MarketClose,
                        MarketTimes{MarketTime{Time{0h, 0min, 0s}, 1}}}},
            .tz                   = UTC, // Using UTC for cryptocurrency markets
            .aliases = {"Crypto", "Cryptocurrency", "Digital_Assets", "Bitcoin", "BTC"},
            // Include all days of the week
            .weekmask = {epoch_core::EpochDayOfWeek::Monday, epoch_core::EpochDayOfWeek::Tuesday,
                         epoch_core::EpochDayOfWeek::Wednesday,
                         epoch_core::EpochDayOfWeek::Thursday, epoch_core::EpochDayOfWeek::Friday,
                         epoch_core::EpochDayOfWeek::Saturday, epoch_core::EpochDayOfWeek::Sunday}};
    };

    const CryptoOptions& crypto_instance()
    {
        static CryptoOptions options;
        return options;
    }

    // Cryptocurrency Exchange Calendar
    CryptoExchangeCalendar::CryptoExchangeCalendar(std::optional<MarketTime> const& open_time,
                                                   std::optional<MarketTime> const& close_time)
        : MarketCalendar(open_time, close_time, crypto_instance().CRYPTO_OPTIONS)
    {
    }
} // namespace epoch_frame::calendar
