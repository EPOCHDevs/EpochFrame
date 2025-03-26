#include "../holidays/nyse.h"
#include "../holidays/us.h"
#include "all.h"
#include "epoch_frame/series.h"
#include "factory/index_factory.h"

namespace epoch_frame::calendar
{
    struct NYSEOptions
    {
        const USHolidays&   us   = USHolidays::Instance();
        const NYSEHolidays& nyse = NYSEHolidays::Instance();

        const DateTime START_DATE     = DateTime{1885y, January, 1d};
        const Time     SATURDAY_CLOSE = Time{12h};
        const DateTime SATURDAY_END   = "1952-09-29"__date.replace_tz(UTC);

        const MarketCalendarOptions NYSE_OPTIONS{
            .name                 = "NYSE",
            .regular_market_times = RegularMarketTimes{{
                {epoch_core::MarketTimeType::Pre, {MarketTime{Time{4h}}}},
                {epoch_core::MarketTimeType::MarketOpen,
                 {MarketTime{Time{10h}, std::nullopt},
                  MarketTime{Time{9h, 30min}, std::nullopt, Date{1985y, January, 1d}}}},
                {epoch_core::MarketTimeType::MarketClose,
                 {MarketTime{Time{15h}},
                  MarketTime{Time{15h, 30min}, std::nullopt, Date{1952y, September, 29d}},
                  MarketTime{Time{16h}, std::nullopt, Date{1974y, January, 16d}}}},
                {epoch_core::MarketTimeType::Post, {MarketTime{Time{20h}}}},
            }},
            .tz                   = EST,
            .regular_holidays     = make_unnamed_calendar({nyse.USNewYearsDayNYSEpost1952,
                                                           nyse.USNewYearsDayNYSEpre1952,
                                                           nyse.USMartinLutherKingJrAfter1998,
                                                           nyse.USPresidentsDay,
                                                           nyse.USWashingtonsBirthDayBefore1952,
                                                           nyse.USWashingtonsBirthDay1952to1963,
                                                           nyse.USWashingtonsBirthDay1964to1970,
                                                           nyse.USLincolnsBirthDayBefore1954,
                                                           nyse.GoodFriday,
                                                           nyse.GoodFridayPre1898,
                                                           nyse.GoodFriday1899to1905,
                                                           nyse.USMemorialDay,
                                                           nyse.USMemorialDayBefore1952,
                                                           nyse.USMemorialDay1952to1964,
                                                           nyse.USMemorialDay1964to1969,
                                                           nyse.USIndependenceDay,
                                                           nyse.USIndependenceDayPre1952,
                                                           nyse.USIndependenceDay1952to1954,
                                                           nyse.USLaborDayStarting1887,
                                                           nyse.USColumbusDayBefore1954,
                                                           nyse.USElectionDay1848to1967,
                                                           nyse.USVeteransDay1934to1953,
                                                           nyse.USThanksgivingDay,
                                                           nyse.USThanksgivingDayBefore1939,
                                                           nyse.USThanksgivingDay1939to1941,
                                                           nyse.ChristmasNYSE,
                                                           nyse.Christmas54to98NYSE,
                                                           nyse.ChristmasBefore1954,
                                                           nyse.USJuneteenthAfter2022},
                                                          START_DATE),
            .adhoc_holidays       = chain(
                // Recurring Holidays
                nyse.SatAfterGoodFridayAdhoc, nyse.MonBeforeIndependenceDayAdhoc,
                nyse.SatBeforeIndependenceDayAdhoc, nyse.SatAfterIndependenceDayAdhoc,
                nyse.DaysAfterIndependenceDayAdhoc, nyse.SatBeforeLaborDayAdhoc,
                nyse.USElectionDay1968to1980Adhoc, nyse.FridayAfterThanksgivingAdhoc,
                nyse.SatBeforeChristmasAdhoc, nyse.SatAfterChristmasAdhoc, nyse.ChristmasEvesAdhoc,
                nyse.DayAfterChristmasAdhoc,
                // Retired
                nyse.USVetransDayAdHoc, nyse.SatAfterColumbusDayAdhoc, nyse.LincolnsBirthDayAdhoc,
                nyse.GrantsBirthDayAdhoc, nyse.SatBeforeNewYearsAdhoc,
                nyse.SatBeforeWashingtonsBirthdayAdhoc, nyse.SatAfterWashingtonsBirthdayAdhoc,
                nyse.SatBeforeAfterLincolnsBirthdayAdhoc, nyse.SatBeforeDecorationAdhoc,
                nyse.SatAfterDecorationAdhoc, nyse.DayBeforeDecorationAdhoc,
                // Irregularities
                nyse.UlyssesGrantFuneral1885, nyse.ColumbianCelebration1892,
                nyse.GreatBlizzardOf1888, nyse.WashingtonInaugurationCentennialCelebration1889,
                nyse.CharterDay1898, nyse.WelcomeNavalCommander1898,
                nyse.AdmiralDeweyCelebration1899, nyse.GarretHobartFuneral1899,
                nyse.QueenVictoriaFuneral1901, nyse.MovedToProduceExchange1901,
                nyse.EnlargedProduceExchange1901, nyse.McKinleyDeathAndFuneral1901,
                nyse.KingEdwardVIIcoronation1902, nyse.NYSEnewBuildingOpen1903,
                nyse.HudsonFultonCelebration1909, nyse.JamesShermanFuneral1912, nyse.OnsetOfWWI1914,
                nyse.WeatherHeatClosing1917, nyse.DraftRegistrationDay1917,
                nyse.WeatherNoHeatClosing1918, nyse.DraftRegistrationDay1918,
                nyse.ArmisticeSigned1918, nyse.Homecoming27Division1919,
                nyse.ParadeOf77thDivision1919, nyse.BacklogRelief1919,
                nyse.GeneralPershingReturn1919, nyse.OfficeLocationChange1920,
                nyse.HardingDeath1923, nyse.HardingFuneral1923, nyse.LindberghParade1927,
                nyse.BacklogRelief1928, nyse.BacklogRelief1929, nyse.CoolidgeFuneral1933,
                nyse.BankHolidays1933, nyse.HeavyVolume1933, nyse.SatClosings1944,
                nyse.RooseveltDayOfMourning1945, nyse.SatClosings1945, nyse.VJday1945,
                nyse.NavyDay1945, nyse.RailroadStrike1946, nyse.SatClosings1946,
                nyse.SatClosings1947, nyse.SatClosings1948, nyse.SevereWeather1948,
                nyse.SatClosings1949, nyse.SatClosings1950, nyse.SatClosings1951,
                nyse.SatClosings1952, nyse.KennedyFuneral1963, nyse.MLKdayOfMourning1968,
                nyse.PaperworkCrisis1968, nyse.SnowClosing1969, nyse.EisenhowerFuneral1969,
                nyse.FirstLunarLandingClosing1969, nyse.TrumanFuneral1972, nyse.JohnsonFuneral1973,
                nyse.NewYorkCityBlackout77, nyse.HurricaneGloriaClosings1985, nyse.NixonFuneral1994,
                nyse.ReaganMourning2004, nyse.FordMourning2007, nyse.September11Closings2001,
                nyse.HurricaneSandyClosings2012, nyse.GeorgeHWBushDeath2018,
                nyse.JimmyCarterDeath2025),
            .aliases  = {"NYSE", "stock", "NASDAQ", "BATS", "DJIA", "DOW"},
            .weekmask = {epoch_core::EpochDayOfWeek::Monday, epoch_core::EpochDayOfWeek::Tuesday,
                         epoch_core::EpochDayOfWeek::Wednesday,
                         epoch_core::EpochDayOfWeek::Thursday, epoch_core::EpochDayOfWeek::Friday},
            .special_opens =
                {
                    SpecialTime{
                        Time{.hour = 11h, .tz = EST},
                        make_unnamed_calendar({nyse.KingEdwardDeath11amyClose1910}, START_DATE)},
                    SpecialTime{Time{.hour = 12h, .tz = EST},
                                make_unnamed_calendar(
                                    {
                                        nyse.ParadeOfNationalGuardEarlyClose1917,
                                        nyse.LibertyDay12pmEarlyClose1917,
                                        nyse.LibertyDay12pmEarlyClose1918,
                                        nyse.WallStreetExplosionEarlyClose1920,
                                        nyse.NRAdemonstration12pmEarlyClose1933,
                                    },
                                    START_DATE)},
                    SpecialTime{.time     = Time{.hour = 12h, .minute = 30min, .tz = EST},
                                .calendar = make_unnamed_calendar(
                                    {
                                        nyse.RooseveltFuneral1230EarlyClose1919,
                                        nyse.WoodrowWilsonFuneral1230EarlyClose1924,
                                        nyse.TaftFuneral1230EarlyClose1930,
                                        nyse.GasFumesOnTradingFloor1230EarlyClose1933,
                                    },
                                    START_DATE)},
                },
            .special_opens_adhoc =
                {
                    SpecialTimeAdHoc{.time     = Time{.hour = 9h, .minute = 31min, .tz = EST},
                                     .calendar = factory::index::make_datetime_index(
                                         nyse.TroopsInGulf931LateOpens1991)},
                    SpecialTimeAdHoc{.time     = Time{.hour = 11h, .tz = EST},
                                     .calendar = factory::index::make_datetime_index(
                                         nyse.HeavyVolume11amLateOpen1933)},
                    SpecialTimeAdHoc{
                        .time     = Time{.hour = 12h, .tz = EST},
                        .calendar = factory::index::make_datetime_index(chain(
                            nyse.BacklogRelief12pmLateOpen1929, nyse.HeavyVolume12pmLateOpen1933))},
                },
            .special_closes       = {},
            .special_closes_adhoc = {}};

        const np::WeekMask WEEKMASK_PRE_1952{true, true, true, true, true, true, false};

        const DateOffsetHandlerPtr HOLIDAYS_PRE_1952 = factory::offset::cbday({
            .weekmask = WEEKMASK_PRE_1952,
            .holidays = NYSE_OPTIONS.adhoc_holidays,
            .calendar = NYSE_OPTIONS.regular_holidays,
        });
    };

    const NYSEOptions& instance()
    {
        static NYSEOptions options;
        return options;
    }

    NYSEExhangeCalendar::NYSEExhangeCalendar(std::optional<MarketTime> const& open_time,
                                             std::optional<MarketTime> const& close_time)
        : MarketCalendar(open_time, close_time, instance().NYSE_OPTIONS)
    {
    }

    IndexPtr NYSEExhangeCalendar::valid_days(const Date& start, const Date& end,
                                             std::string const& tz) const
    {
        auto start_date = DateTime{start}.tz_localize(tz);
        auto end_date   = DateTime{end}.tz_localize(tz);

        DateTime saturday_end = tz == "" ? instance().SATURDAY_END.tz_localize("") : start_date;

        if (start_date > saturday_end)
        {
            return MarketCalendar::valid_days(start, end, tz);
        }

        if (end_date <= saturday_end)
        {
            return factory::index::date_range({.start  = start_date.timestamp(),
                                               .end    = end_date.timestamp(),
                                               .offset = instance().HOLIDAYS_PRE_1952,
                                               .tz     = tz});
        }

        auto days_pre  = factory::index::date_range({.start  = start_date.timestamp(),
                                                     .end    = saturday_end.timestamp(),
                                                     .offset = instance().HOLIDAYS_PRE_1952,
                                                     .tz     = tz});
        auto days_post = factory::index::date_range({.start  = saturday_end.timestamp(),
                                                     .end    = end_date.timestamp(),
                                                     .offset = m_holidays,
                                                     .tz     = tz});

        return days_pre->union_(days_post);
    }

    Series NYSEExhangeCalendar::days_at_time(IndexPtr const&          days_,
                                             const MarketTimeVariant& market_time,
                                             int64_t                  day_offset) const
    {
        auto days = MarketCalendar::days_at_time(days_, market_time, day_offset);

        if (std::holds_alternative<epoch_core::MarketTimeType>(market_time))
        {
            auto type = std::get<epoch_core::MarketTimeType>(market_time);
            if (type == epoch_core::MarketTimeType::MarketClose && !is_custom(type))
            {
                days =
                    Series(days.index(),
                           days.dt()
                               .tz_convert(m_options.tz)
                               .where(days.dt().day_of_week(arrow::compute::DayOfWeekOptions{}) !=
                                          5_scalar,
                                      days.dt().normalize() +
                                          Scalar(_tdelta(instance().SATURDAY_CLOSE, std::nullopt)))
                               .dt()
                               .tz_convert(UTC)
                               .value(),
                           "");
            }
        }

        return days;
    }

    IndexPtr NYSEExhangeCalendar::date_range_htf(Date const& start, Date const& end,
                                                 std::optional<int64_t> periods) const
    {
        if (start > instance().SATURDAY_END.date)
        {
            return MarketCalendar::date_range_htf(start, end, periods);
        }

        throw std::runtime_error(
            "NYSEExhangeCalendar::date_range_htf is not implemented for dates before 1952");
    }

} // namespace epoch_frame::calendar