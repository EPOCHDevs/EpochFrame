#include "calendar/calendar_common.h"
#include "calendar/calendars/all.h"
#include "date_time/business/np_busdaycal.h"
#include "date_time/datetime.h"
#include "epoch_frame/factory/index_factory.h"
#include "epoch_frame/factory/scalar_factory.h"
#include "epoch_frame/factory/series_factory.h"
#include "epoch_frame/scalar.h"
#include "epoch_frame/series.h"
#include <catch.hpp>
#include <chrono>
#include <memory>
#include <variant>

using namespace epoch_frame::factory;
using namespace epoch_frame::factory::scalar;
using namespace epoch_frame;

TEST_CASE("CME Globex FX Calendar", "[calendar]")
{
    using namespace epoch_frame::calendar;
    using namespace epoch_frame;
    static CMEGlobexFXExchangeCalendar cal;

    SECTION("test_time_zone")
    {
        REQUIRE(cal.tz() == "America/Chicago");
        REQUIRE(cal.name() == "CMEGlobex_FX");
    }

    SECTION("test_sunday_opens")
    {
        // Test that the market opens on Sunday evening
        auto schedule = cal.schedule("2020-01-12"__date.date, "2020-01-31"__date.date, {});

        // Monday's session should open on Sunday at 5 PM Chicago time
        auto expected = schedule.loc(Scalar{"2020-01-13"_date}, "MarketOpen");

        REQUIRE(expected.to_datetime() ==
                "2020-01-12 17:00:00"__dt.replace_tz(UTC) + TimeDelta{{.hours = 6}});
    }

    SECTION("test_2020_through_2022_and_prior_holidays")
    {
        struct TestCase
        {
            DateTime                                       date;
            std::variant<Time, epoch_core::MarketTimeType> market_time;
        };

        static std::vector<TestCase> test_cases = {
            // 2020
            // 2020 Martin Luther King Day (20th = Monday)
            {"2020-01-17"__date, epoch_core::MarketTimeType::MarketOpen},
            {"2020-01-20"__date, Time{12h, 0min}},
            {"2020-01-21"__date, epoch_core::MarketTimeType::MarketOpen},
            // 2020 Presidents Day (17th = Monday)
            {"2020-02-14"__date, epoch_core::MarketTimeType::MarketOpen},
            {"2020-02-17"__date, Time{12h, 0min}},
            {"2020-02-18"__date, epoch_core::MarketTimeType::MarketOpen},
            // 2020 Good Friday (10th = Friday)
            {"2020-04-09"__date, epoch_core::MarketTimeType::MarketOpen},
            {"2020-04-10"__date, epoch_core::MarketTimeType::MarketClose},
            {"2020-04-13"__date, epoch_core::MarketTimeType::MarketOpen},
            // 2020 Memorial Day (May 25 = Monday)
            {"2020-05-22"__date, epoch_core::MarketTimeType::MarketOpen},
            {"2020-05-25"__date, Time{12h, 0min}},
            {"2020-05-26"__date, epoch_core::MarketTimeType::MarketOpen},
            // 2020 Independence Day (4th = Saturday)
            {"2020-07-02"__date, epoch_core::MarketTimeType::MarketOpen},
            {"2020-07-03"__date, Time{12h, 0min}},
            {"2020-07-06"__date, epoch_core::MarketTimeType::MarketOpen},
            // 2020 Labor Day (7th = Monday)
            {"2020-09-04"__date, epoch_core::MarketTimeType::MarketOpen},
            {"2020-09-07"__date, Time{12h, 0min}},
            {"2020-09-08"__date, epoch_core::MarketTimeType::MarketOpen},
            // 2020 Thanksgiving (26th = Thursday)
            {"2020-11-25"__date, epoch_core::MarketTimeType::MarketOpen},
            {"2020-11-26"__date, Time{12h, 0min}},
            {"2020-11-27"__date, Time{12h, 15min}},
            {"2020-11-30"__date, epoch_core::MarketTimeType::MarketOpen},
            // 2020 Christmas (25th = Friday)
            {"2020-12-24"__date, Time{12h, 15min}},
            {"2020-12-25"__date, epoch_core::MarketTimeType::MarketClose},
            {"2020-12-28"__date, epoch_core::MarketTimeType::MarketOpen},
            {"2020-12-29"__date, epoch_core::MarketTimeType::MarketOpen},
            // 2020/21 New Year's (Jan 1 = Friday)
            {"2020-12-31"__date, epoch_core::MarketTimeType::MarketOpen},
            {"2021-01-01"__date, epoch_core::MarketTimeType::MarketClose},
            {"2021-01-04"__date, epoch_core::MarketTimeType::MarketOpen},
            // 2021
            // 2021 Martin Luther King Day (18th = Monday)
            {"2021-01-15"__date, epoch_core::MarketTimeType::MarketOpen},
            {"2021-01-18"__date, Time{12h, 0min}},
            {"2021-01-19"__date, epoch_core::MarketTimeType::MarketOpen},
            // 2021 Presidents Day (15th = Monday)
            {"2021-02-12"__date, epoch_core::MarketTimeType::MarketOpen},
            {"2021-02-15"__date, Time{12h, 0min}},
            {"2021-02-16"__date, epoch_core::MarketTimeType::MarketOpen},
            // 2021 Good Friday (2nd = Friday)
            {"2021-04-01"__date, epoch_core::MarketTimeType::MarketOpen},
            {"2021-04-02"__date, Time{10h, 15min}},
            {"2021-04-05"__date, epoch_core::MarketTimeType::MarketOpen},
            // 2021 Memorial Day (May 31 = Monday)
            {"2021-05-28"__date, epoch_core::MarketTimeType::MarketOpen},
            {"2021-05-31"__date, Time{12h, 0min}},
            {"2021-06-01"__date, epoch_core::MarketTimeType::MarketOpen},
            // 2021 Independence Day (4th = Sunday)
            {"2021-07-02"__date, epoch_core::MarketTimeType::MarketOpen},
            {"2021-07-05"__date, Time{12h, 0min}},
            {"2021-07-06"__date, epoch_core::MarketTimeType::MarketOpen},
            // 2021 Labor Day (6th = Monday)
            {"2021-09-03"__date, epoch_core::MarketTimeType::MarketOpen},
            {"2021-09-06"__date, Time{12h, 0min}},
            {"2021-09-07"__date, epoch_core::MarketTimeType::MarketOpen},
            // 2021 Thanksgiving (25th = Thursday)
            {"2021-11-24"__date, epoch_core::MarketTimeType::MarketOpen},
            {"2021-11-25"__date, Time{12h, 0min}},
            {"2021-11-26"__date, Time{12h, 15min}},
            // 2021 Christmas (25th = Saturday)
            {"2021-12-23"__date, epoch_core::MarketTimeType::MarketOpen},
            {"2021-12-24"__date, epoch_core::MarketTimeType::MarketClose},
            {"2021-12-27"__date, epoch_core::MarketTimeType::MarketOpen},
            // 2021 / 22 New Year's (Dec 31 = Friday) (unusually this period was fully open)
            {"2021-12-31"__date, epoch_core::MarketTimeType::MarketOpen},
            {"2022-01-03"__date, epoch_core::MarketTimeType::MarketOpen},
            {"2022-01-04"__date, epoch_core::MarketTimeType::MarketOpen},
            // 2022
            // 2022 Martin Luther King Day (17th = Monday)
            {"2022-01-14"__date, epoch_core::MarketTimeType::MarketOpen},
            {"2022-01-17"__date, epoch_core::MarketTimeType::MarketOpen},
            {"2022-01-18"__date, epoch_core::MarketTimeType::MarketOpen},
            // 2022 President's Day (21st = Monday)
            {"2022-02-18"__date, epoch_core::MarketTimeType::MarketOpen},
            {"2022-02-21"__date, epoch_core::MarketTimeType::MarketOpen},
            {"2022-02-22"__date, epoch_core::MarketTimeType::MarketOpen},
            // 2022 Good Friday (15 = Friday)
            {"2022-04-14"__date, epoch_core::MarketTimeType::MarketOpen},
            {"2022-04-15"__date, epoch_core::MarketTimeType::MarketClose},
            {"2022-04-18"__date, epoch_core::MarketTimeType::MarketOpen},
            // 2022 Memorial Day (30th = Monday)
            {"2022-05-27"__date, epoch_core::MarketTimeType::MarketOpen},
            {"2022-05-30"__date, epoch_core::MarketTimeType::MarketOpen},
            {"2022-05-31"__date, epoch_core::MarketTimeType::MarketOpen},
            // 2022 Juneteenth (20th = Monday)
            {"2022-06-17"__date, epoch_core::MarketTimeType::MarketOpen},
            {"2022-06-20"__date, epoch_core::MarketTimeType::MarketOpen},
            {"2022-06-21"__date, epoch_core::MarketTimeType::MarketOpen},
            // 2022 Independence Day (4th = Monday)
            {"2022-07-01"__date, epoch_core::MarketTimeType::MarketOpen},
            {"2022-07-04"__date, epoch_core::MarketTimeType::MarketOpen},
            {"2022-07-05"__date, epoch_core::MarketTimeType::MarketOpen},
            // 2022 Labor Day (5th = Monday)
            {"2022-09-02"__date, epoch_core::MarketTimeType::MarketOpen},
            {"2022-09-05"__date, epoch_core::MarketTimeType::MarketOpen},
            {"2022-09-06"__date, epoch_core::MarketTimeType::MarketOpen},
            // 2022 Thanksgiving (24th = Thursday)
            {"2022-11-23"__date, epoch_core::MarketTimeType::MarketOpen},
            {"2022-11-24"__date, epoch_core::MarketTimeType::MarketOpen},
            {"2022-11-25"__date, Time{12h, 15min}},
            {"2022-11-28"__date, epoch_core::MarketTimeType::MarketOpen},
            // 2022 Christmas (25 = Sunday)
            {"2022-12-23"__date, epoch_core::MarketTimeType::MarketOpen},
            {"2022-12-26"__date, epoch_core::MarketTimeType::MarketClose},
            {"2022-12-27"__date, epoch_core::MarketTimeType::MarketOpen},
            // 2022/23 New Years (Jan 1 = Sunday)
            {"2022-12-30"__date, epoch_core::MarketTimeType::MarketOpen},
            {"2023-01-02"__date, epoch_core::MarketTimeType::MarketClose},
            {"2023-01-03"__date, epoch_core::MarketTimeType::MarketOpen},
            // 2023 Good Friday (7 = Friday)
            {"2023-04-07"__date, Time{10h, 15min}}};

        auto schedule = cal.schedule("2020-01-01"__date.date, "2023-04-28"__date.date, {.tz = CST});
        for (const auto& [date_, market_time] : test_cases)
        {
            std::visit(
                [&]<typename T>(T const& market_time)
                {
                    auto date = date_.replace_tz(CST);
                    DYNAMIC_SECTION("date: " << date << " market_time: " << market_time)
                    {
                        auto   date_scalar = Scalar{date};
                        auto   offset      = date_scalar.dt().is_dst().as_bool() ? 5.0 : 6.0;
                        auto   delta       = TimeDelta{{.hours = offset}};
                        Scalar day_ts{date + delta};

                        if constexpr (std::is_same_v<T, Time>)
                        {
                            auto market_open  = schedule.loc(date_scalar, "MarketOpen");
                            auto market_close = schedule.loc(date_scalar, "MarketClose");
                            auto expected_date =
                                offset::hours(17)->add(offset::days(-1)->add(day_ts.timestamp()));

                            Scalar expected_scalar{expected_date};

                            // SECTION("market_open")
                            {
                                INFO("expected_scalar: " << expected_scalar.repr());
                                REQUIRE(market_open == expected_scalar);
                            }

                            expected_date =
                                offset::minutes(market_time.minute.count())
                                    ->add(offset::hours(market_time.hour.count())
                                              ->add(offset::days(0)->add(day_ts.timestamp())));
                            expected_scalar = Scalar{expected_date};

                            // SECTION("market_close")
                            {
                                INFO("expected_scalar: " << expected_scalar.repr());
                                REQUIRE(market_close == expected_scalar);
                            }
                        }
                        else if constexpr (std::is_same_v<T, epoch_core::MarketTimeType>)
                        {
                            if (market_time == epoch_core::MarketTimeType::MarketOpen)
                            {
                                auto market_open  = schedule.loc(date_scalar, "MarketOpen");
                                auto market_close = schedule.loc(date_scalar, "MarketClose");

                                auto expected_open_date =
                                    offset::minutes(0)->add(offset::hours(17)->add(
                                        offset::days(-1)->add(day_ts.timestamp())));
                                Scalar expected_open_scalar{expected_open_date};

                                // SECTION("market_open")
                                {
                                    INFO("expected_open_scalar: " << expected_open_scalar.repr());
                                    REQUIRE(market_open == expected_open_scalar);
                                }

                                auto expected_close_date =
                                    offset::minutes(0)->add(offset::hours(16)->add(
                                        offset::days(0)->add(day_ts.timestamp())));
                                Scalar expected_close_scalar{expected_close_date};

                                // SECTION("market_close")
                                {
                                    INFO("expected_close_scalar: " << expected_close_scalar.repr());
                                    REQUIRE(market_close == expected_close_scalar);
                                }
                            }
                            else
                            {
                                REQUIRE_FALSE(
                                    schedule.index()->contains(day_ts.dt().tz_localize("")));
                            }
                        }
                    }
                },
                market_time);
        }
    }
}
