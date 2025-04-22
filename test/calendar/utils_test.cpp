//
// Created by adesola on 4/8/25.
//
#include "calendar/calendars/all.h"
#include "epoch_frame/calendar_utils.h"
#include "epoch_frame/factory/calendar_factory.h"
#include "epoch_frame/factory/dataframe_factory.h"
#include "epoch_frame/factory/date_offset_factory.h"
#include "epoch_frame/factory/index_factory.h"
#include "epoch_frame/factory/scalar_factory.h"
#include "epoch_frame/scalar.h"
#include "epoch_frame/series.h"
#include "epoch_frame/time_delta.h"
#include <catch.hpp>

using namespace epoch_frame::factory;
using namespace epoch_frame::factory::scalar;
using namespace epoch_frame;
using namespace epoch_frame::calendar;

// Test fixture for market calendars with breaks
class CalendarUtilsTest
{
  public:
    CalendarUtilsTest()
    {
        // Setup calendars for testing
        nyse_calendar = CalendarFactory::instance().get_calendar("NYSE");
        fx_calendar   = CalendarFactory::instance().get_calendar("FX");
    }

    std::shared_ptr<MarketCalendar> nyse_calendar;
    std::shared_ptr<MarketCalendar> fx_calendar;
};

TEST_CASE_METHOD(CalendarUtilsTest, "Merge Schedules Test")
{
    SECTION("test_merge_schedules_outer")
    {
        // Generate schedules for two different calendars
        // NYSE is closed on July 4th, FX trades on July 4th
        auto nyse_schedule =
            nyse_calendar->schedule("2023-07-03"__date.date(), "2023-07-07"__date.date(), {});
        auto fx_schedule =
            fx_calendar->schedule("2023-07-03"__date.date(), "2023-07-07"__date.date(), {});

        INFO("NYSE Schedule:");
        INFO(nyse_schedule.repr());
        INFO("FX Schedule:");
        INFO(fx_schedule.repr());

        // Merge with outer join - should include all dates from both calendars
        auto merged_outer = utils::merge_schedules({nyse_schedule, fx_schedule}, true);

        INFO("Merged Outer Schedule:");
        INFO(merged_outer.repr());

        // Verify July 4th is included in the merged schedule
        REQUIRE(merged_outer.index()->contains(Scalar{"2023-07-04"_date}));

        // Verify all dates from both calendars are included
        auto all_dates = factory::index::date_range(
            {.start = "2023-07-03"_date, .end = "2023-07-07"_date, .offset = offset::days(1)});

        for (int i = 0; i < all_dates->size(); i++)
        {
            auto date = all_dates->at(i);
            REQUIRE(merged_outer.index()->contains(date));
        }
    }

    SECTION("test_merge_schedules_inner")
    {
        // Generate schedules for two different calendars
        auto nyse_schedule =
            nyse_calendar->schedule("2023-07-03"__date.date(), "2023-07-07"__date.date(), {});
        auto fx_schedule =
            fx_calendar->schedule("2023-07-03"__date.date(), "2023-07-07"__date.date(), {});

        // Merge with inner join - should only include dates common to both calendars
        auto merged_inner = utils::merge_schedules({nyse_schedule, fx_schedule}, false);

        INFO("Merged Inner Schedule:");
        INFO(nyse_schedule.repr());
        INFO(fx_schedule.repr());
        INFO(merged_inner.repr());

        // July 4th should be excluded as NYSE is closed
        REQUIRE_FALSE(merged_inner.index()->contains(Scalar{"2023-07-04"_date}));

        // Other trading days should be included
        REQUIRE(merged_inner.index()->contains(Scalar{"2023-07-03"_date}));
        REQUIRE(merged_inner.index()->contains(Scalar{"2023-07-05"_date}));
        REQUIRE(merged_inner.index()->contains(Scalar{"2023-07-06"_date}));
        REQUIRE(merged_inner.index()->contains(Scalar{"2023-07-07"_date}));
    }

    SECTION("test_merge_schedules_with_breaks")
    {
        // NYSE has lunch breaks, FX doesn't
        auto nyse_schedule =
            nyse_calendar->schedule("2023-07-03"__date.date(), "2023-07-07"__date.date(), {});
        auto fx_schedule =
            fx_calendar->schedule("2023-07-03"__date.date(), "2023-07-07"__date.date(), {});

        // Add break columns to NYSE schedule for testing
        Series break_start = nyse_schedule["MarketOpen"] + Scalar{TimeDelta{{.hours = 2}}};
        Series break_end   = nyse_schedule["MarketOpen"] + Scalar{TimeDelta{{.hours = 3}}};

        DataFrame nyse_with_breaks = nyse_schedule;
        nyse_with_breaks           = nyse_with_breaks.assign("BreakStart", break_start);
        nyse_with_breaks           = nyse_with_breaks.assign("BreakEnd", break_end);

        INFO("NYSE Schedule with Breaks:");
        INFO(nyse_with_breaks.repr());

        // Merge schedules - breaks should be preserved in the result
        auto merged = utils::merge_schedules({nyse_with_breaks, fx_schedule}, true);

        INFO("Merged Schedule with Breaks:");
        INFO(merged.repr());

        // Verify break columns are NOT retained
        std::vector<std::string> columns = merged.column_names();
        bool                     has_break_start =
            std::find(columns.begin(), columns.end(), "BreakStart") != columns.end();
        bool has_break_end = std::find(columns.begin(), columns.end(), "BreakEnd") != columns.end();

        REQUIRE_FALSE(has_break_start);
        REQUIRE_FALSE(has_break_end);
    }

    SECTION("test_merge_same_calendar")
    {
        // Merging the same calendar should return identical schedule
        auto schedule1 =
            nyse_calendar->schedule("2023-07-03"__date.date(), "2023-07-07"__date.date(), {});
        auto schedule2 =
            nyse_calendar->schedule("2023-07-03"__date.date(), "2023-07-07"__date.date(), {});

        auto merged = utils::merge_schedules({schedule1, schedule2}, false);

        INFO("Original Schedule:");
        INFO(schedule1.repr());
        INFO("Merged Schedule:");
        INFO(merged.repr());

        // Verify merged schedule is identical to original
        REQUIRE(merged.shape()[0] == schedule1.shape()[0]);
        REQUIRE(merged.equals(schedule1));
    }
}
