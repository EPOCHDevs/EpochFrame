#include "../holidays/cme_globex.h"
#include "../holidays/cme.h"
#include "../holidays/us.h"
#include "all.h"
#include "epoch_frame/calendar_common.h"
#include "date_time/holiday/holiday.h"
#include "date_time/holiday/holiday_calendar.h"
#include "epoch_frame/aliases.h"

#define us USHolidays::Instance()
#define cme CMEHolidays::Instance()
#define cme_globex CMEGlobexHolidays::Instance()
namespace epoch_frame::calendar
{
    const Time _1015 = Time{10h, 15min};
    const Time _1200 = Time{12h};
    const Time _1215 = Time{12h, 15min};

    struct CMEGlobexBaseOptions
    {
        // Define common elements for all calendars
        const AbstractHolidayCalendarPtr common_holidays =
            make_unnamed_calendar({us.USNewYearsDay, GoodFriday, us.Christmas});

        const AbstractHolidayCalendarPtr common_special_closes =
            make_unnamed_calendar({us.USMartinLutherKingJrAfter1998, us.USPresidentsDay,
                                   us.USMemorialDay, us.USJuneteenthAfter2022, us.USIndependenceDay,
                                   USLaborDay, us.USThanksgivingDay, us.USBlackFridayInOrAfter1993,
                                   us.ChristmasEveBefore1993, us.ChristmasEveInOrAfter1993},
                                  DateTime{1900y, January, 1d});

        inline static const auto common_weekmask = {
            epoch_core::EpochDayOfWeek::Monday, epoch_core::EpochDayOfWeek::Tuesday,
            epoch_core::EpochDayOfWeek::Wednesday, epoch_core::EpochDayOfWeek::Thursday,
            epoch_core::EpochDayOfWeek::Friday};
    };

    const CMEGlobexBaseOptions& cme_globex_base_instance()
    {
        static CMEGlobexBaseOptions options;
        return options;
    }

    struct CMEGlobexOptions
    {
        // FX (Currency) Exchange Calendar
        const MarketCalendarOptions GLOBEX_FX_OPTIONS{
            .name                 = "CMEGlobex_FX",
            .regular_market_times = RegularMarketTimes{{
                {epoch_core::MarketTimeType::MarketOpen, {MarketTime{Time{17h}, -1}}},
                {epoch_core::MarketTimeType::MarketClose, {MarketTime{Time{16h}}}},
            }},
            .tz                   = CST,
            .regular_holidays     = make_unnamed_calendar(
                {us.USNewYearsDay, cme.GoodFridayBefore2021, cme.GoodFriday2022, us.Christmas}),
            .adhoc_holidays = {},
            .aliases        = {"CMEGlobex_FX", "CME_FX", "CME_Currency"},
            .weekmask       = cme_globex_base_instance().common_weekmask,
            .special_closes = {
                {_1015, make_unnamed_calendar({cme.GoodFridayAfter2022, cme.GoodFriday2021})},
                {_1200, make_unnamed_calendar({
                            cme.USMartinLutherKingJrAfter1998Before2022,
                            cme.USPresidentsDayBefore2022,
                            cme.USMemorialDay2021AndPrior,
                            cme.USIndependenceDayBefore2022,
                            cme.USLaborDayStarting1887Before2022,
                            cme.USThanksgivingBefore2022,
                        })},
                {_1215,
                 make_unnamed_calendar({cme.USThanksgivingFriday, us.ChristmasEveInOrAfter1993})}}};

        // Crypto Exchange Calendar
        const MarketCalendarOptions GLOBEX_CRYPTO_OPTIONS{
            .name                 = "CME Globex Crypto",
            .regular_market_times = RegularMarketTimes{{
                {epoch_core::MarketTimeType::MarketOpen,
                 {MarketTime{Time{.hour = 17h, .tz = CST}, -1}}},
                {epoch_core::MarketTimeType::MarketClose,
                 {MarketTime{Time{.hour = 16h, .tz = CST}}}},
                {epoch_core::MarketTimeType::BreakStart,
                 {MarketTime{Time{.hour = 16h, .tz = CST}}}},
                {epoch_core::MarketTimeType::BreakEnd, {MarketTime{Time{.hour = 17h, .tz = CST}}}},
            }},
            .tz                   = CST,
            .regular_holidays     = make_unnamed_calendar({
                cme.GoodFridayBefore2021,
                cme.GoodFriday2022,
                cme_globex.ChristmasCME,
                us.USNewYearsDay,
            }),
            .adhoc_holidays       = {},
            .aliases              = {"CME Globex Cryptocurrencies", "CME Globex Crypto"},
            .weekmask             = cme_globex_base_instance().common_weekmask,
            .special_closes       = {
                SpecialTime{Time{.hour = 8h, .minute = 15min, .tz = CST},
                            make_unnamed_calendar({cme.GoodFriday2021})},
                {Time{.hour = 10h, .minute = 15min, .tz = CST},
                       make_unnamed_calendar({cme.GoodFridayAfter2022})},
                {Time{.hour = 12h, .tz = CST},
                       make_unnamed_calendar(
                     {cme_globex.USMartinLutherKingJrPre2022, cme_globex.USPresidentsDayPre2022,
                            cme_globex.USMemorialDayPre2022, cme_globex.USIndependenceDayPre2022,
                            cme_globex.USLaborDayPre2022, cme_globex.USThanksgivingDayPre2022})},
                    {Time{.hour = 12h, .minute = 15min, .tz = CST},
                        make_unnamed_calendar({us.ChristmasEveInOrAfter1993,
                            cme.USIndependenceDayBefore2022PreviousDay,
                            cme_globex.USThanksgivingFridayPre2021})},
                {Time{.hour = 12h, .minute = 45min, .tz = CST},
                       make_unnamed_calendar({cme_globex.USThanksgivingFridayFrom2021})},
                {Time{.hour = 16h, .tz = CST},
                       make_unnamed_calendar(
                     {cme_globex.USMartinLutherKingJrFrom2022, cme_globex.USPresidentsDayFrom2022,
                            cme_globex.USMemorialDayFrom2022, cme_globex.USJuneteenthFrom2022,
                            cme_globex.USIndependenceDayFrom2022, cme_globex.USLaborDayFrom2022,
                            cme_globex.USThanksgivingDayFrom2022})}}};

        // Equities Exchange Calendar
        const MarketCalendarOptions GLOBEX_EQUITIES_OPTIONS{
            .name                 = "CME Globex Equities",
            .regular_market_times = RegularMarketTimes{{
                {epoch_core::MarketTimeType::MarketOpen, {MarketTime{Time{17h}, -1}}},
                {epoch_core::MarketTimeType::MarketClose, {MarketTime{Time{16h}}}},
            }},
            .tz                   = CST,
            .regular_holidays =
                make_unnamed_calendar({us.USNewYearsDay, cme.GoodFridayBefore2021NotEarlyClose,
                                       cme.GoodFriday2022, us.Christmas}),
            .adhoc_holidays = {},
            .aliases        = {"CME Globex Equity"},
            .weekmask       = cme_globex_base_instance().common_weekmask,
            .special_closes = {
                SpecialTime{
                    Time{.hour = 10h, .minute = 30min},
                    make_unnamed_calendar(
                        {cme.USMartinLutherKingJrAfter1998Before2015, cme.USPresidentsDayBefore2015,
                         cme.USMemorialDay2013AndPrior, cme.USIndependenceDayBefore2014,
                         cme.USLaborDayStarting1887Before2014, cme.USThanksgivingBefore2014})},
                SpecialTime{Time{.hour = 12h, .minute = 15min},
                            make_unnamed_calendar({cme.USIndependenceDayBefore2022PreviousDay,
                                                   cme.USThanksgivingFriday,
                                                   us.ChristmasEveInOrAfter1993})},
                SpecialTime{Time{.hour = 12h},
                            make_unnamed_calendar(
                                {cme.USMartinLutherKingJrAfter2015, cme.USPresidentsDayAfter2015,
                                 cme.USMemorialDayAfter2013, cme.USIndependenceDayAfter2014,
                                 cme.USLaborDayStarting1887After2014, cme.USThanksgivingAfter2014,
                                 us.USJuneteenthAfter2022})},
                SpecialTime{Time{.hour = 8h, .minute = 15min},
                            make_unnamed_calendar({cme.GoodFriday2010, cme.GoodFriday2012,
                                                   cme.GoodFriday2015, cme.GoodFriday2021,
                                                   cme.GoodFridayAfter2022})}}};

        // Livestock Exchange Calendar
        const MarketCalendarOptions GLOBEX_LIVESTOCK_OPTIONS{
            .name                 = "CMEGlobex_Livestock",
            .regular_market_times = RegularMarketTimes{{
                {epoch_core::MarketTimeType::MarketOpen, {MarketTime{Time{8h, 30min}}}},
                {epoch_core::MarketTimeType::MarketClose, {MarketTime{Time{13h, 5min}}}},
            }},
            .tz                   = CST,
            .regular_holidays     = make_unnamed_calendar(
                {us.USNewYearsDay, us.USMartinLutherKingJrAfter1998, us.USPresidentsDay, GoodFriday,
                     us.USMemorialDay, us.USIndependenceDay, USLaborDay, us.USThanksgivingDay,
                     us.Christmas}),
            .adhoc_holidays = {},
            .aliases  = {"CMEGlobex_Livestock", "CMEGlobex_Live_Cattle", "CMEGlobex_Feeder_Cattle",
                         "CMEGlobex_Lean_Hog", "CMEGlobex_Port_Cutout"},
            .weekmask = cme_globex_base_instance().common_weekmask,
            .special_closes = {SpecialTime{
                Time{.hour = 12h, .minute = 5min},
                make_unnamed_calendar({us.USBlackFridayInOrAfter1993, us.ChristmasEveBefore1993,
                                       us.ChristmasEveInOrAfter1993})}}};

        // Grains and Oilseeds Exchange Calendar
        const MarketCalendarOptions GLOBEX_GRAINS_OPTIONS{
            .name                 = "CMEGlobex_GrainsAndOilseeds",
            .regular_market_times = RegularMarketTimes{{
                {epoch_core::MarketTimeType::MarketOpen, {MarketTime{Time{19h}, -1}}},
                {epoch_core::MarketTimeType::MarketClose, {MarketTime{Time{13h, 20min}}}},
                {epoch_core::MarketTimeType::BreakStart, {MarketTime{Time{7h, 45min}}}},
                {epoch_core::MarketTimeType::BreakEnd, {MarketTime{Time{8h, 30min}}}},
            }},
            .tz                   = CST,
            .regular_holidays     = make_unnamed_calendar(
                {us.USNewYearsDay, us.USMartinLutherKingJrAfter1998, us.USPresidentsDay, GoodFriday,
                     us.USMemorialDay, us.USIndependenceDay, USLaborDay, us.USThanksgivingDay,
                     us.Christmas}),
            .adhoc_holidays = {},
            .aliases        = {"CMEGlobex_Grains", "CMEGlobex_Oilseeds"},
            .weekmask       = cme_globex_base_instance().common_weekmask,
            .special_closes = {}};

        // Fixed Income Exchange Calendar
        const MarketCalendarOptions GLOBEX_FIXED_INCOME_OPTIONS{
            .name                 = "CME Globex Fixed Income",
            .regular_market_times = RegularMarketTimes{{
                {epoch_core::MarketTimeType::MarketOpen, {MarketTime{Time{18h}, -1}}},
                {epoch_core::MarketTimeType::MarketClose, {MarketTime{Time{17h}}}},
            }},
            .tz                   = CST,
            .regular_holidays =
                make_unnamed_calendar({us.USNewYearsDay, cme.GoodFridayBefore2021NotEarlyClose,
                                       cme.GoodFriday2022, us.Christmas}),
            .adhoc_holidays = {},
            .aliases        = {"CME Globex Fixed Income", "CME Globex Interest Rate Products"},
            .weekmask       = cme_globex_base_instance().common_weekmask,
            .special_closes =
                {SpecialTime{
                     Time{.hour = 12h},
                     make_unnamed_calendar(
                         {cme.USMartinLutherKingJrAfter1998Before2015,
                          cme.USMartinLutherKingJrAfter2015, cme.USPresidentsDayBefore2015,
                          cme.USPresidentsDayAfter2015, cme.USMemorialDay2013AndPrior,
                          cme.USMemorialDayAfter2013, cme.USIndependenceDayBefore2014,
                          cme.USIndependenceDayAfter2014, cme.USLaborDayStarting1887Before2014,
                          cme.USLaborDayStarting1887After2014, cme.USThanksgivingBefore2014,
                          cme.USThanksgivingAfter2014, us.USJuneteenthAfter2022})},
                 SpecialTime{Time{.hour = 15h, .minute = 15min},
                             make_unnamed_calendar(
                                 {cme.USMartinLutherKingJrAfter1998Before2016FridayBefore,
                                  cme.USPresidentsDayBefore2016FridayBefore, cme.GoodFriday2009,
                                  cme.USMemorialDay2015AndPriorFridayBefore,
                                  cme.USLaborDayStarting1887Before2015FridayBefore})},
                 SpecialTime{Time{.hour = 12h, .minute = 15min},
                             make_unnamed_calendar(
                                 {cme.USThanksgivingFriday, us.ChristmasEveInOrAfter1993})},
                 SpecialTime{Time{.hour = 10h, .minute = 15min, .tz = CST},
                             make_unnamed_calendar({cme.GoodFriday2010, cme.GoodFriday2012,
                                                    cme.GoodFriday2015, cme.GoodFriday2021,
                                                    cme.GoodFridayAfter2022})}},
            .special_closes_adhoc =
                {SpecialTimeAdHoc{.time     = Time{.hour = 15h, .minute = 15min},
                                  .calendar = factory::index::make_datetime_index(
                                      {"2010-07-02"__date, "2011-07-01"__date})},
                 SpecialTimeAdHoc{.time = Time{.hour = 12h, .minute = 15min},
                                  .calendar =
                                      factory::index::make_datetime_index({"2010-12-31"__date})}},
        };

        // Energy and Metals Exchange Calendar
        const MarketCalendarOptions GLOBEX_ENERGY_METALS_OPTIONS{
            .name                 = "CMEGlobex_EnergyAndMetals",
            .regular_market_times = RegularMarketTimes{{
                {epoch_core::MarketTimeType::MarketOpen, {MarketTime{Time{17h}, -1}}},
                {epoch_core::MarketTimeType::MarketClose, {MarketTime{Time{16h}}}},
            }},
            .tz                   = CST,
            .regular_holidays =
                make_unnamed_calendar({us.USNewYearsDay, GoodFriday, cme_globex.ChristmasCME}),
            .adhoc_holidays = {},
            .aliases        = {"CMEGlobex_EnergyAndMetals",
                               "CMEGlobex_Energy",
                               "CMEGlobex_CrudeAndRefined",
                               "CMEGlobex_NYHarbor",
                               "CMEGlobex_HO",
                               "HO",
                               "CMEGlobex_Crude",
                               "CMEGlobex_CL",
                               "CL",
                               "CMEGlobex_Gas",
                               "CMEGlobex_RB",
                               "RB",
                               "CMEGlobex_MicroCrude",
                               "CMEGlobex_MCL",
                               "MCL",
                               "CMEGlobex_NatGas",
                               "CMEGlobex_NG",
                               "NG",
                               "CMEGlobex_Dutch_NatGas",
                               "CMEGlobex_TTF",
                               "TTF",
                               "CMEGlobex_LastDay_NatGas",
                               "CMEGlobex_NN",
                               "NN",
                               "CMEGlobex_CarbonOffset",
                               "CMEGlobex_CGO",
                               "CGO",
                               "C-GEO",
                               "CMEGlobex_NGO",
                               "NGO",
                               "CMEGlobex_GEO",
                               "GEO",
                               "CMEGlobex_Metals",
                               "CMEGlobex_PreciousMetals",
                               "CMEGlobex_Gold",
                               "CMEGlobex_GC",
                               "GC",
                               "CMEGlobex_Silver",
                               "CMEGlobex_SI",
                               "SI",
                               "CMEGlobex_Platinum",
                               "CMEGlobex_PL",
                               "PL",
                               "CMEGlobex_BaseMetals",
                               "CMEGlobex_Copper",
                               "CMEGlobex_HG",
                               "HG",
                               "CMEGlobex_Aluminum",
                               "CMEGlobex_ALI",
                               "ALI",
                               "CMEGlobex_QC",
                               "QC",
                               "CMEGlobex_FerrousMetals",
                               "CMEGlobex_HRC",
                               "HRC",
                               "CMEGlobex_BUS",
                               "BUS",
                               "CMEGlobex_TIO",
                               "TIO"},
            .weekmask       = cme_globex_base_instance().common_weekmask,
            .special_closes = {
                // For pre-2022 dates
                SpecialTime{
                    Time{.hour = 12h, .tz = CST},
                    make_unnamed_calendar(
                        {cme_globex.USMartinLutherKingJrPre2022, cme_globex.USPresidentsDayPre2022,
                         cme_globex.USMemorialDayPre2022, cme_globex.USIndependenceDayPre2022,
                         cme_globex.USLaborDayPre2022, cme_globex.USThanksgivingDayPre2022})},
                // For Friday after Thanksgiving
                SpecialTime{Time{.hour = 12h, .minute = 45min, .tz = CST},
                            make_unnamed_calendar({cme_globex.FridayAfterThanksgiving})},
                // For 2022 and later dates
                SpecialTime{Time{.hour = 13h, .minute = 30min, .tz = CST},
                            make_unnamed_calendar({cme_globex.USMartinLutherKingJrFrom2022,
                                                   cme_globex.USPresidentsDayFrom2022,
                                                   cme_globex.USMemorialDayFrom2022,
                                                   cme_globex.USJuneteenthFrom2022,
                                                   cme_globex.USIndependenceDayFrom2022,
                                                   cme_globex.USThanksgivingDayFrom2022})}}};
    };

    const CMEGlobexOptions& cme_globex_instance()
    {
        static CMEGlobexOptions options;
        return options;
    }

    // CME Globex FX Exchange Calendar
    CMEGlobexFXExchangeCalendar::CMEGlobexFXExchangeCalendar(
        std::optional<MarketTime> const& open_time, std::optional<MarketTime> const& close_time)
        : MarketCalendar(open_time, close_time, cme_globex_instance().GLOBEX_FX_OPTIONS)
    {
    }

    // CME Globex Crypto Exchange Calendar
    CMEGlobexCryptoExchangeCalendar::CMEGlobexCryptoExchangeCalendar(
        std::optional<MarketTime> const& open_time, std::optional<MarketTime> const& close_time)
        : MarketCalendar(open_time, close_time, cme_globex_instance().GLOBEX_CRYPTO_OPTIONS)
    {
    }

    // CME Globex Equities Exchange Calendar
    CMEGlobexEquitiesExchangeCalendar::CMEGlobexEquitiesExchangeCalendar(
        std::optional<MarketTime> const& open_time, std::optional<MarketTime> const& close_time)
        : MarketCalendar(open_time, close_time, cme_globex_instance().GLOBEX_EQUITIES_OPTIONS)
    {
    }

    // CME Globex Livestock Exchange Calendar
    CMEGlobexLivestockExchangeCalendar::CMEGlobexLivestockExchangeCalendar(
        std::optional<MarketTime> const& open_time, std::optional<MarketTime> const& close_time)
        : MarketCalendar(open_time, close_time, cme_globex_instance().GLOBEX_LIVESTOCK_OPTIONS)
    {
    }

    // CME Globex Grains And Oilseeds Exchange Calendar
    CMEGlobexGrainsAndOilseedsExchangeCalendar::CMEGlobexGrainsAndOilseedsExchangeCalendar(
        std::optional<MarketTime> const& open_time, std::optional<MarketTime> const& close_time)
        : MarketCalendar(open_time, close_time, cme_globex_instance().GLOBEX_GRAINS_OPTIONS)
    {
    }

    // CME Globex Fixed Income Exchange Calendar
    CMEGlobexFixedIncomeCalendar::CMEGlobexFixedIncomeCalendar(
        std::optional<MarketTime> const& open_time, std::optional<MarketTime> const& close_time)
        : MarketCalendar(open_time, close_time, cme_globex_instance().GLOBEX_FIXED_INCOME_OPTIONS)
    {
    }

    // CME Globex Energy and Metals Exchange Calendar
    CMEGlobexEnergyAndMetalsExchangeCalendar::CMEGlobexEnergyAndMetalsExchangeCalendar(
        std::optional<MarketTime> const& open_time, std::optional<MarketTime> const& close_time)
        : MarketCalendar(open_time, close_time, cme_globex_instance().GLOBEX_ENERGY_METALS_OPTIONS)
    {
    }

} // namespace epoch_frame::calendar
