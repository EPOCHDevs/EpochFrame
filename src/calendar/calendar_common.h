#pragma once

#include <unordered_map>
#include <any>
#include <epoch_core/enum_wrapper.h>
#include "date_time/datetime.h"
#include "date_time/holiday/holiday.h"
#include "calendar_utils.h"


CREATE_ENUM(MarketTimeType,
    MarketOpen,
    MarketClose,
    BreakStart,
    BreakEnd,
    Pre,
    Post,
    InternalUseOnly
);

// Define enum for OpenCloseType with serialization support
CREATE_ENUM(OpenCloseType,
            Default, // Use default behavior defined by the class
            True,    // Time opens the market (equivalent to true)
            False    // Time closes the market (equivalent to false)
);

CREATE_ENUM(BooleanEnum, True, False);

namespace epoch_frame::calendar {

    template<typename T>
    using ProtectedDict = std::unordered_map<epoch_core::MarketTimeType, T>;

    struct NoMarketTime {};
    struct AllMarketTimes {};
    using MarketTimeFilter = std::variant<std::vector<epoch_core::MarketTimeType>, NoMarketTime, AllMarketTimes>;

    struct ScheduleOptions {
        std::string const& tz{"UTC"};
        epoch_core::MarketTimeType start{epoch_core::MarketTimeType::MarketOpen};
        epoch_core::MarketTimeType end{epoch_core::MarketTimeType::MarketClose};
        epoch_core::BooleanEnum force_special_times{epoch_core::BooleanEnum::True};
        MarketTimeFilter market_times{NoMarketTime{}};
        bool interruptions{false};
    };
    // MarketTime structure with optional cutoff date
    struct MarketTime
    {
        std::optional<Time>    time{std::nullopt};
        std::optional<int64_t> day_offset{std::nullopt};
        std::optional<Date>    date{std::nullopt};
    };

    struct MarketTimeWithTZ
    {
        Time    time;
        std::optional<int64_t> day_offset{std::nullopt};
        std::optional<Date>    date{std::nullopt};
    };

    struct MarketTimeDelta
    {
        std::optional<Date> date{std::nullopt};
        TimeDelta           time_delta;
    };

    // For representing sequences of market times (for time transitions)
    using MarketTimes        = std::vector<MarketTime>;
    using MarketTimesWithTZ = std::vector<MarketTimeWithTZ>;

    using RegularMarketTimes = ProtectedDict<MarketTimes>;
    using RegularMarketTimesWithTZ = ProtectedDict<MarketTimesWithTZ>;
    using OpenCloseMap       = ProtectedDict<epoch_core::OpenCloseType>;
    using namespace std::chrono_literals;

    struct Interruption
    {
        Date       date;
        MarketTime start_time;
        MarketTime end_time;
    };
    using Interruptions = std::vector<Interruption>;

    // Or continue using std::optional<bool> as a simpler alternative
    using OpensType = std::optional<bool>;

    struct SpecialTime
    {
        Time                       time;
        AbstractHolidayCalendarPtr calendar;
        int64_t                    day_offset{0};
    };

    struct SpecialTimeAdHoc
    {
        Time                       time;
        IndexPtr calendar;
        int64_t                    day_offset{0};
    };


    using SpecialTimes = std::vector<SpecialTime>;
    using SpecialTimesAdHoc = std::vector<SpecialTimeAdHoc>;
    using MarketTimeVariant = std::variant<Time, epoch_core::MarketTimeType>;

    struct MarketCalendarOptions {
        std::string        name;
        RegularMarketTimes regular_market_times;
        OpenCloseMap       open_close_map{};
        std::string        tz{"UTC"};

        AbstractHolidayCalendarPtr regular_holidays{nullptr};
        np::HolidayList            adhoc_holidays{};
        std::vector<std::string>   aliases{};
        np::WeekSet                weekmask{epoch_core::EpochDayOfWeek::Monday, epoch_core::EpochDayOfWeek::Tuesday,
            epoch_core::EpochDayOfWeek::Wednesday, epoch_core::EpochDayOfWeek::Thursday,
            epoch_core::EpochDayOfWeek::Friday};
        SpecialTimes               special_opens{};
        SpecialTimesAdHoc          special_opens_adhoc{};
        SpecialTimes               special_closes{};
        SpecialTimesAdHoc          special_closes_adhoc{};
        Interruptions              interruptions{};
    };

}

