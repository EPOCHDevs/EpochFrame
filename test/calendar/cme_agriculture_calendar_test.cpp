#include "calendar/calendar_common.h"
#include "calendar/calendars/all.h"
#include "date_time/business/np_busdaycal.h"
#include "epoch_frame/factory/index_factory.h"
#include "epoch_frame/factory/scalar_factory.h"
#include "epoch_frame/factory/series_factory.h"
#include "epoch_frame/scalar.h"
#include "epoch_frame/series.h"
#include <catch.hpp>
#include <memory>

using namespace epoch_frame::factory;
using namespace epoch_frame::factory::scalar;
using namespace epoch_frame;

TEST_CASE("CME Agriculture Calendar", "[calendar]")
{
    using namespace epoch_frame::calendar;
    using namespace epoch_frame;
    static CMEAgricultureExchangeCalendar cal;

    SECTION("test_time_zone")
    {
        REQUIRE(cal.tz() == "America/Chicago");
        REQUIRE(cal.name() == "CME_Agriculture");
    }

    SECTION("test_2020_holidays")
    {
        // martin luthur king: 2020-01-20
        // president's day: 2020-02-17
        // good friday: 2020-04-10
        // memorial day: 2020-05-25
        // independence day: 2020-04-02 and 2020-04-03 (not implemented as holidays)
        // labor day: 2020-09-07
        // thanksgiving: 2020-11-25, 2020-11-26
        // christmas (observed): 2020-12-25, 2020-12-27
        // new years (observed): 2021-01-01

        auto good_dates = cal.valid_days("2020-01-01"__date.date, "2021-01-10"__date.date);

        std::vector<DateTime> expected_holidays = {
            "2020-01-20 00:00:00"__dt, "2020-02-17 00:00:00"__dt, "2020-04-10 00:00:00"__dt,
            "2020-05-25 00:00:00"__dt, "2020-09-07 00:00:00"__dt, "2020-11-26 00:00:00"__dt,
            "2020-12-25 00:00:00"__dt, "2020-12-27 00:00:00"__dt, "2021-01-01 00:00:00"__dt};

        // Verify none of the holidays are in valid days
        for (const auto& holiday : expected_holidays)
        {
            INFO("Testing holiday: " << holiday);
            auto holiday_utc = holiday.replace_tz("UTC");
            REQUIRE_FALSE(good_dates->contains(Scalar(holiday_utc)));
        }

        // Verify dates that should be included
        std::vector<DateTime> expected_trading_days = {
            "2020-04-02 00:00:00"__dt, "2020-04-03 00:00:00"__dt, "2020-11-25 00:00:00"__dt};

        for (const auto& trading_day : expected_trading_days)
        {
            INFO("Testing trading day: " << trading_day);
            auto trading_day_utc = trading_day.replace_tz("UTC");
            REQUIRE(good_dates->contains(Scalar(trading_day_utc)));
        }
    }

    SECTION("test_dec_jan")
    {
        auto schedule = cal.schedule("2020-12-30"__date.date, "2021-01-10"__date.date, {});

        // First market open should be 2020-12-29 23:01:00 UTC
        auto first_open = schedule.iloc(0, "MarketOpen").to_datetime();
        REQUIRE(first_open.replace_tz("UTC") == "2020-12-29 23:01:00"__dt.replace_tz("UTC"));

        // Last market close should be 2021-01-08 23:00:00 UTC
        auto last_close = schedule.iloc(schedule.num_rows() - 1, "MarketClose").to_datetime();
        REQUIRE(last_close.replace_tz("UTC") == "2021-01-08 23:00:00"__dt.replace_tz("UTC"));
    }
}
