#include "calendar/calendars/all.h"
#include "epoch_frame/datetime.h"
#include "epoch_core/macros.h"
#include "epoch_frame/factory/dataframe_factory.h"
#include "epoch_frame/factory/index_factory.h"
#include "epoch_frame/factory/scalar_factory.h"
#include "epoch_frame/factory/series_factory.h"
#include "epoch_frame/scalar.h"
#include "epoch_frame/series.h"
#include <catch.hpp>
#include <catch2/catch_test_macros.hpp>
#include <chrono>
#include <memory>
#include <variant>

using namespace epoch_frame::factory;
using namespace epoch_frame::factory::scalar;
using namespace epoch_frame;

TEST_CASE("CME Globex Energy and Metals Calendar", "[calendar]")
{
    using namespace epoch_frame::calendar;
    using namespace epoch_frame;
    static const CMEGlobexEnergyAndMetalsExchangeCalendar cal;

    SECTION("test_time_zone")
    {
        REQUIRE(cal.tz() == CST);
        REQUIRE(cal.name() == "CMEGlobex_EnergyAndMetals");
    }

    SECTION("test_open_time_tz")
    {
        REQUIRE(cal.open_time().at(0).time.tz == cal.tz());
    }

    SECTION("test_close_time_tz")
    {
        REQUIRE(cal.close_time().at(0).time.tz == cal.tz());
    }

    SECTION("test_weekmask")
    {
        using namespace epoch_core;
        REQUIRE(cal.weekmask().contains(EpochDayOfWeek::Monday));
        REQUIRE(cal.weekmask().contains(EpochDayOfWeek::Tuesday));
        REQUIRE(cal.weekmask().contains(EpochDayOfWeek::Wednesday));
        REQUIRE(cal.weekmask().contains(EpochDayOfWeek::Thursday));
        REQUIRE(cal.weekmask().contains(EpochDayOfWeek::Friday));
        REQUIRE_FALSE(cal.weekmask().contains(EpochDayOfWeek::Saturday));
        REQUIRE_FALSE(cal.weekmask().contains(EpochDayOfWeek::Sunday));
    }

    auto test_holidays =
        [](const std::vector<DateTime>& holidays, DateTime const& start, DateTime const& end)
    {
        auto index       = factory::index::make_datetime_index(cal.holidays()->holidays());
        auto index_array = index->array();
        auto mask        = (index_array >= Scalar(start)) && (index_array <= Scalar(end));
        index            = index->loc(mask);

        REQUIRE(holidays.size() == index->size());

        auto df =
            make_dataframe(index->tz_localize(UTC), {index->as_chunked_array()}, {"holidays"});
            auto expected = index::make_datetime_index(holidays);
            INFO("Holidays: " << df.index()->repr() << "\nExpected: \n" << expected->repr());
        REQUIRE(df.index()->equals(expected));

        auto valid_days = cal.valid_days(start.date, end.date);
        for (auto const& h : holidays)
        {
            REQUIRE_FALSE(valid_days->contains(Scalar(h)));
        }
    };

    SECTION("test 2022")
    {
        auto start = "2022-01-01"__date;
        auto end   = "2022-12-31"__date;

        test_holidays(
            {
                "2022-04-15"__date.replace_tz("UTC"),
                "2022-12-26"__date.replace_tz("UTC"),
            },
            start, end);
    }

    SECTION("test 2021")
    {
        auto start = "2021-01-01"__date;
        auto end   = "2021-12-31"__date;

        test_holidays(
            {
                "2021-01-01"__date.replace_tz("UTC"),
                "2021-04-02"__date.replace_tz("UTC"),
                "2021-12-24"__date.replace_tz("UTC"),
            },
            start, end);
    }

    SECTION("test 2020")
    {
        auto start = "2020-01-01"__date;
        auto end   = "2020-12-31"__date;

        test_holidays(
            {
                "2020-01-01"__date.replace_tz("UTC"),
                "2020-04-10"__date.replace_tz("UTC"),
                "2020-12-25"__date.replace_tz("UTC"),
            },
            start, end);
    }
}
