#include "calendar/calendars/all.h"
#include "epoch_frame/datetime.h"
#include "epoch_frame/factory/index_factory.h"
#include "epoch_frame/factory/scalar_factory.h"
#include "epoch_frame/scalar.h"
#include "epoch_frame/series.h"
#include <catch.hpp>
#include <memory>


using namespace epoch_frame::factory;
using namespace epoch_frame::factory::scalar;
using namespace epoch_frame;

TEST_CASE("CME Globex Grains and Oilseeds Calendar", "[calendar]")
{
    using namespace epoch_frame::calendar;
    using namespace epoch_frame;
    static CMEGlobexGrainsAndOilseedsExchangeCalendar cal;

    SECTION("test_time_zone")
    {
        REQUIRE(cal.tz() == "America/Chicago");
        REQUIRE(cal.name() == "CMEGlobex_GrainsAndOilseeds");
    }

    SECTION("test_x")
    {
        auto good_dates = cal.valid_days("2023-01-01"__date.date(), "2023-12-31"__date.date());

        // Dates that should be holidays
        std::vector<DateTime> expected_holidays = {"2023-01-01"__date, "2023-12-24"__date,
                                                   "2023-12-25"__date, "2023-12-30"__date,
                                                   "2023-12-31"__date};

        for (const auto& date : expected_holidays)
        {
            INFO("Testing holiday: " << date);
            REQUIRE_FALSE(good_dates->contains(Scalar(date.replace_tz(UTC))));
        }

        // Dates that should be trading days
        std::vector<DateTime> expected_trading_days = {"2023-01-03"__date, "2023-01-05"__date,
                                                       "2023-12-26"__date, "2023-12-27"__date,
                                                       "2023-12-28"__date};

        for (const auto& date : expected_trading_days)
        {
            INFO("Testing trading day: " << date);
            REQUIRE(good_dates->contains(Scalar(date.replace_tz(UTC))));
        }
    }
}
